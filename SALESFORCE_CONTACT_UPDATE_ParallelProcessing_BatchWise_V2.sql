CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pyjwt', 'cryptography', 'requests', 'simplejson', 'snowflake-snowpark-python', 'pandas')
SECRETS = ('cred' = UAT_DB_MANAGER.SECRETS.SALESFORCE_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
import json
import time
import requests
import snowflake.snowpark as snowpark
import _snowflake
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- constants preserved from original ---
MAX_WORKERS = 15
BIG_BATCH_SIZE = 2000
COOLDOWN_SECONDS = 120
MAX_RETRIES = 3
RETRY_DELAYS = [1, 3, 6]

# --- helper: refresh token (MAIN THREAD) - same as original ---
def refresh_auth_token(session):
    try:
        res = session.sql(
            "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
        ).collect()
        if res:
            return list(res[0].values())[0]
    except Exception:
        return None
    return None

# NOTE: build_headers below will NOT read secrets when called inside workers.
# We will read subscriptionID ONCE on the main thread and keep it in a variable.
def build_headers_from_cached(token, subscriptionID):
    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

# Worker function - MUST NOT use session or _snowflake.
def send_one_worker(rec_id, req_obj, base_url, token_getter, subscriptionID):
    """
    Runs in worker thread. Uses only:
      - rec_id (string)
      - req_obj (dict) -> prebuilt on main thread
      - base_url (string)
      - token_getter() -> reads in-memory token (no Snowflake calls)
      - subscriptionID (cached string)
    Does NOT call session or _snowflake.
    """
    last_status = None
    last_resp = None

    for attempt in range(MAX_RETRIES):
        try:
            token = token_getter()  # read-only access to in-memory token
            headers = build_headers_from_cached(token, subscriptionID)
            resp = requests.put(base_url, headers=headers, json=req_obj, timeout=60)

            status = getattr(resp, "status_code", None)
            last_status = status
            try:
                last_resp = resp.json()
            except Exception:
                last_resp = {"raw": resp.text if hasattr(resp, "text") else str(resp)}

            # handle transient/unauthorized responses - retry loop will attempt
            if status == 401:
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                continue

            if status in (429, 500, 502, 503, 504):
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                continue

            # success or non-retriable status
            break

        except Exception as e_req:
            last_status = None
            last_resp = {"exception": str(e_req)}
            time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
            continue

    return {
        "id": rec_id,
        "request": req_obj,
        "status": last_status,
        "response": last_resp
    }


def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):
    load_nbr = None
    delta_end = None
    app_db = None
    app_schema = None
    ids = []

    try:
        # --- config read (main thread) ---
        cfg_q = f"""
            SELECT SALESFORCE_UPDATE_FROM_APP_DB AS DBNAME,
                   SALESFORCE_UPDATE_FROM_APP_SCHEMA AS SCHEMA_NAME
            FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
            WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT'
              AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
            LIMIT 1
        """
        cfg = session.sql(cfg_q).collect()
        if not cfg:
            raise Exception("Missing config row for CONTACT")

        app_db = cfg[0]["DBNAME"]
        app_schema = cfg[0]["SCHEMA_NAME"]

        # load number + delta window
        load_nbr = session.sql(
            f"SELECT {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS NBR"
        ).collect()[0]["NBR"]

        prev_delta_q = f"""
            SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            WHERE LOAD_STATUS = 1
        """
        prev_delta = session.sql(prev_delta_q).collect()[0]["PREV"]

        delta_end_q = f"""
            SELECT MAX(ROW_CRE_DT) AS END_TS
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        """
        delta_end_res = session.sql(delta_end_q).collect()[0]["END_TS"]
        delta_end = delta_end_res if delta_end_res else session.sql("SELECT CURRENT_TIMESTAMP() AS T").collect()[0]["T"]

        # insert control row (in-progress)
        session.sql(f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE,
             LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES ({load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL,
                    0, CURRENT_TIMESTAMP(), 'SFADMIN')
        """).collect()

        # read API base URL (main thread)
        url_row = session.sql(f"""
            SELECT URL
            FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
            WHERE TARGET_TABLE_NAME = 'SALESFORCE_CONTACT_DATA_UPDATE'
            LIMIT 1
        """).collect()
        if not url_row:
            raise Exception("Missing API endpoint configuration")
        base_url = url_row[0]["URL"]

        # read subscription secret ONCE (main thread) and cache
        try:
            credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
        except Exception:
            credentials = {}
        subscriptionID = credentials.get("subscriptionID")

        # initial token via auth proc (main thread)
        token = refresh_auth_token(session)
        token_holder = [token]    # mutable holder used by token_getter/refresh_and_update

        def token_getter():
            return token_holder[0]

        def refresh_and_update():
            new_token = refresh_auth_token(session)
            if new_token:
                token_holder[0] = new_token
            return token_holder[0]

        # listing of IDs for this delta (main thread)
        listing_q = f"""
            SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID AS ID
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE ROW_CRE_DT > '{prev_delta}'
              AND ROW_CRE_DT <= '{delta_end}'
            ORDER BY ROW_CRE_DT
        """
        rows = session.sql(listing_q).collect()
        ids = [r["ID"] for r in rows]
        total = len(ids)

        if total == 0:
            return "No records to process"

        # iterate in BIG_BATCH chunks (prefetch per BIG_BATCH)
        for batch_num, start in enumerate(range(0, total, BIG_BATCH_SIZE), start=1):
            big_batch = ids[start:start + BIG_BATCH_SIZE]
            print(f"Starting BIG BATCH {batch_num} — {len(big_batch)} records")

            # PREFETCH payloads for big_batch on MAIN THREAD only
            # Build list of (rec_id, request_obj)
            prebuilt_payloads = []
            for rec_id in big_batch:
                payload_q = f"""
                    SELECT OBJECT_CONSTRUCT(* EXCLUDE (
                        SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID
                    )) AS RECORD_DATA,
                    CONTACTID
                    FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
                    WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID = '{rec_id}'
                """
                rec_rows = session.sql(payload_q).collect()
                if not rec_rows:
                    # record disappeared; still include empty payload to track failure
                    prebuilt_payloads.append((rec_id, {}))
                    continue

                rec_obj = rec_rows[0]["RECORD_DATA"]
                contactid = rec_rows[0].get("CONTACTID")

                # convert variant to dict safely
                if isinstance(rec_obj, str):
                    try:
                        req_obj = json.loads(rec_obj)
                    except Exception:
                        try:
                            req_obj = json.loads(rec_obj.replace("'", '"'))
                        except Exception:
                            req_obj = {}
                else:
                    try:
                        req_obj = dict(rec_obj)
                    except Exception:
                        req_obj = json.loads(json.dumps(rec_obj))

                if contactid:
                    req_obj["CONTACTID"] = contactid

                prebuilt_payloads.append((rec_id, req_obj))

            # Now process prebuilt_payloads in small chunks using MAX_WORKERS threads
            audit_entries = []

            for s in range(0, len(prebuilt_payloads), MAX_WORKERS):
                chunk = prebuilt_payloads[s:s+MAX_WORKERS]

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {
                        pool.submit(send_one_worker, rec_id, req_obj, base_url, token_getter, subscriptionID): rec_id
                        for rec_id, req_obj in chunk
                    }

                    for fut in as_completed(futures):
                        try:
                            audit_entries.append(fut.result())
                        except Exception as e:
                            audit_entries.append({
                                "id": futures[fut],
                                "request": {},
                                "status": None,
                                "response": {"exception": str(e)}
                            })

            # Bulk insert audit entries (same format as original)
            if audit_entries:
                values = []
                for a in audit_entries:
                    rid = a["id"]
                    try:
                        req_text = json.dumps(a["request"])
                    except Exception:
                        req_text = str(a["request"])
                    req_sql = req_text.replace("'", "''")
                    try:
                        resp_text = json.dumps(a["response"])
                    except Exception:
                        resp_text = str(a["response"])
                    resp_sql = resp_text.replace("'", "''")
                    status_val = a["status"]
                    status_sql = str(status_val) if status_val is not None else "NULL"

                    values.append(
                        f"('{rid}','{req_sql}','{status_sql}','{resp_sql}',CURRENT_TIMESTAMP(),'SFADMIN')"
                    )

                session.sql(f"""
                    INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
                    (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON,
                     ROW_CRE_DT, ROW_CRE_USR_ID)
                    VALUES {','.join(values)}
                """).collect()

            # After each big batch, refresh token (main thread) and cooldown (same as original)
            refresh_and_update()

            try:
                session.sql(f"CALL SYSTEM$WAIT({COOLDOWN_SECONDS})").collect()
            except Exception:
                time.sleep(COOLDOWN_SECONDS)

    except Exception as e:
        # Keep original behavior: print error and allow finally block to update control table
        print("Top-level exception:", e)

    finally:
        # Preserve finally block EXACTLY: update LOAD_STATUS = 1 irrespective of success/failure
        if load_nbr and app_db and app_schema and delta_end:
            session.sql(f"""
                UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
                SET LOAD_STATUS = 1,
                    DELTA_END_DATE = '{delta_end}',
                    LOAD_END_DATE = CURRENT_TIMESTAMP()
                WHERE LOAD_NBR = {load_nbr}
            """).collect()

    return f"Completed CONTACT update | LOAD_NBR={load_nbr} | Records={len(ids)}"
$$;
