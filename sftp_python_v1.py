"""
sftp_to_blob_standalone.py
--------------------------
Standalone SFTP -> Azure Blob copier for Matillion (Python Script component,
Interpreter = Python 3) or any shell.

  * ONE SFTP login, N worker threads (one SFTP channel each) -> parallel copy
  * Picks files ending in .xml OR with no extension (skips archive/ folder)
  * Upload to Blob -> move source file to archive folder on SFTP
  * Retries each file 3x, writes a manifest CSV to Blob, and raises an error
    if any file failed (Matillion component turns red)

HOW TO USE
  1. Fill in every <PLACEHOLDER> in the CONFIG block below.
  2. Paste the whole script into a Matillion Python Script component
     (Interpreter = Python 3) and run it.
  Needs on the Matillion server:  pip3 install paramiko azure-storage-blob

Speed: with 8 workers, 10,000 small files typically take ~4-8 min over an
on-prem -> Azure link. Actual speed depends on network latency and on the
SFTP server's per-user session limit (MaxSessions). Start with 8 workers.
"""

# ==========================================================================
# CONFIG  -  fill these in before running
# ==========================================================================
CONFIG = {
    # ---- SFTP source ----
    "host":        "<SFTP_HOST>",          # e.g. mms-dts-pd.xxxx.com
    "port":        22,
    "user":        "<SFTP_USERNAME>",
    "password":    "<SFTP_PASSWORD>",      # or set to None and use key_file
    "key_file":    None,                   # e.g. "/home/matillion/.ssh/id_rsa"
    "remote_dir":  "<SFTP_SOURCE_FOLDER>", # e.g. /LMS
    "archive_dir": "<SFTP_ARCHIVE_FOLDER>",# e.g. /LMS/archive ; None = don't move
    "recursive":   False,                  # True = also read sub-folders

    # ---- Azure Blob target ----
    "blob_conn_str":   "<AZURE_STORAGE_CONNECTION_STRING>",
                       # DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...
                       # or a SAS connection string: BlobEndpoint=...;SharedAccessSignature=...
    "container":       "<BLOB_CONTAINER>",     # e.g. landing
    "blob_prefix":     "<BLOB_TARGET_FOLDER>", # e.g. LMS/2026/11/01  ("" = container root)
    "manifest_prefix": "_manifests",           # where the run manifest CSV goes

    # ---- Tuning ----
    "workers":       8,      # parallel threads; keep BELOW the SFTP MaxSessions limit
    "max_files":     None,   # e.g. 50000 to cap one run (history load); None = all
    "pattern":       r"(?i)^(?:.*\.xml|[^.]+)$",   # .xml or no extension
    "overwrite":     True,   # overwrite blob if it already exists
    "fail_on_error": True,   # raise at the end if any file failed
}
# ==========================================================================

import csv
import io
import os
import posixpath
import re
import stat
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import paramiko
from azure.storage.blob import BlobServiceClient, ContentSettings


# --------------------------------------------------------------------------
# SFTP connection: one login, one SFTP channel per thread
# --------------------------------------------------------------------------
class SftpPool:
    def __init__(self, host, port, username, password=None, key_file=None,
                 timeout=30):
        self.transport = paramiko.Transport((host, port))
        self.transport.banner_timeout = timeout
        self.transport.set_keepalive(30)
        pkey = None
        if key_file:
            pkey = paramiko.RSAKey.from_private_key_file(key_file)
        self.transport.connect(username=username, password=password, pkey=pkey)
        self._local = threading.local()
        self._clients = []
        self._lock = threading.Lock()

    def client(self):
        """Return this thread's own SFTP channel (created on first use)."""
        c = getattr(self._local, "sftp", None)
        if c is None:
            c = paramiko.SFTPClient.from_transport(self.transport)
            self._local.sftp = c
            with self._lock:
                self._clients.append(c)
        return c

    def close(self):
        for c in self._clients:
            try:
                c.close()
            except Exception:
                pass
        self.transport.close()


# --------------------------------------------------------------------------
# Listing
# --------------------------------------------------------------------------
def list_files(sftp, remote_dir, pattern, max_files=None, recursive=False,
               exclude_dirs=("archive",)):
    """Return [(full_path, size)] of regular files whose NAME matches pattern."""
    rx = re.compile(pattern)
    out = []
    stack = [remote_dir]
    while stack:
        d = stack.pop()
        for e in sftp.listdir_iter(d):
            full = posixpath.join(d, e.filename)
            if stat.S_ISDIR(e.st_mode):
                if recursive and e.filename.lower() not in exclude_dirs:
                    stack.append(full)
                continue
            if not stat.S_ISREG(e.st_mode):
                continue
            if rx.match(e.filename):
                out.append((full, e.st_size))
                if max_files and len(out) >= max_files:
                    return out
    return out


# --------------------------------------------------------------------------
# Per-file work (runs in a worker thread)
# --------------------------------------------------------------------------
def copy_one(pool, container_client, remote_path, remote_root, blob_prefix,
             archive_dir, overwrite, retries=3):
    rel = posixpath.relpath(remote_path, remote_root)
    blob_name = posixpath.join(blob_prefix, rel) if blob_prefix else rel
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            sftp = pool.client()
            buf = io.BytesIO()
            sftp.getfo(remote_path, buf)
            data = buf.getvalue()

            container_client.upload_blob(
                name=blob_name, data=data, overwrite=overwrite,
                content_settings=ContentSettings(content_type="application/xml"),
            )

            if archive_dir:
                target = posixpath.join(archive_dir, posixpath.basename(remote_path))
                try:
                    sftp.posix_rename(remote_path, target)  # overwrites if exists
                except (IOError, paramiko.SSHException):
                    sftp.rename(remote_path, target)
            return {"file": remote_path, "blob": blob_name, "bytes": len(data),
                    "status": "OK", "attempts": attempt, "error": ""}
        except Exception as e:  # retry on transient errors
            last_err = e
            if "BlobAlreadyExists" in str(e):
                return {"file": remote_path, "blob": blob_name, "bytes": 0,
                        "status": "SKIPPED_EXISTS", "attempts": attempt,
                        "error": ""}
            time.sleep(2 * attempt)
    return {"file": remote_path, "blob": blob_name, "bytes": 0,
            "status": "FAILED", "attempts": retries, "error": repr(last_err)}


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def run(cfg):
    t0 = time.time()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    workers = int(cfg["workers"])

    blob_svc = BlobServiceClient.from_connection_string(cfg["blob_conn_str"])
    container = blob_svc.get_container_client(cfg["container"])

    pool = SftpPool(cfg["host"], int(cfg["port"]), cfg["user"],
                    password=cfg.get("password"), key_file=cfg.get("key_file"))
    try:
        files = list_files(pool.client(), cfg["remote_dir"], cfg["pattern"],
                           max_files=cfg.get("max_files"),
                           recursive=cfg.get("recursive", False))
        print(f"[{run_id}] {len(files)} files matched in {cfg['remote_dir']}"
              f" (workers={workers})")

        if cfg.get("archive_dir"):
            try:
                pool.client().mkdir(cfg["archive_dir"])
            except IOError:
                pass  # already exists

        results = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(copy_one, pool, container, path, cfg["remote_dir"],
                              cfg.get("blob_prefix", ""), cfg.get("archive_dir"),
                              cfg.get("overwrite", True))
                    for path, _ in files]
            for i, f in enumerate(as_completed(futs), 1):
                results.append(f.result())
                if i % 100 == 0 or i == len(futs):
                    print(f"  progress {i}/{len(futs)}")
    finally:
        pool.close()

    ok = sum(r["status"] == "OK" for r in results)
    skipped = sum(r["status"] == "SKIPPED_EXISTS" for r in results)
    failed = [r for r in results if r["status"] == "FAILED"]
    secs = time.time() - t0

    # manifest
    if cfg.get("manifest_prefix") is not None and results:
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)
        mname = posixpath.join(cfg["manifest_prefix"], f"manifest_{run_id}.csv")
        container.upload_blob(mname, out.getvalue().encode(), overwrite=True)
        print(f"Manifest: {cfg['container']}/{mname}")

    print(f"Done in {secs:.1f}s  OK={ok}  SKIPPED={skipped}  FAILED={len(failed)}")
    for r in failed[:20]:
        print(f"  FAILED {r['file']}: {r['error']}")

    summary = {"run_id": run_id, "matched": len(files), "ok": ok,
               "skipped": skipped, "failed": len(failed), "seconds": round(secs, 1)}
    if failed and cfg.get("fail_on_error", True):
        raise RuntimeError(f"{len(failed)} file(s) failed - see log/manifest")
    return summary



# --------------------------------------------------------------------------
# Validate config and run
# --------------------------------------------------------------------------
def validate(cfg):
    missing = [k for k, v in cfg.items() if isinstance(v, str) and v.startswith("<")]
    if missing:
        raise ValueError("Fill in CONFIG placeholders first: " + ", ".join(missing))
    if not cfg.get("password") and not cfg.get("key_file"):
        raise ValueError("Set either password or key_file in CONFIG")
    if int(cfg["workers"]) < 1:
        raise ValueError("workers must be >= 1")


validate(CONFIG)
print(run(CONFIG))
