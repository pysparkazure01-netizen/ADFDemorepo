CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_UPDATE(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'main'
EXECUTE AS OWNER
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pyjwt','cryptography','requests','simplejson','snowflake-snowpark-python','pandas')
SECRETS = ('cred' = UAT_DB_MANAGER.SECRETS.SALESFORCE_API_INTEGRATION_SECRET)
AS
$$
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.snowpark as snowpark
import _snowflake

# ----------------------------------------------------------------------
# Helper: send one account record to endpoint (all exceptions are swallowed)
# ----------------------------------------------------------------------
def send_single(session, app_db, app_schema, rec_id, base_url, headers):
    try:
        # Fetch payload variant for the given record id
        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID = '{rec_id}'
        """
        rec_rows = session.sql(payload_q).collect()
        if not rec_rows:
            request_obj = {}
        else:
            rec_obj = rec_rows[0]["RECORD_DATA"]
            # Convert VARIANT to dict safely
            if isinstance(rec_obj, dict):
                request_obj = rec_obj
            else:
                try:
                    request_obj = dict(rec_obj)
                except Exception:
                    # fallback to JSON parse
                    try:
                        request_obj = json.loads(str(rec_obj))
                    except Exception:
                        request_obj = {}

        # Send HTTP PUT (we follow Contact proc pattern). All exceptions are captured.
        try:
            resp = requests.put(base_url, headers=headers, json=request_obj, timeout=60)
            status = getattr(resp, "status_code", None)
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"raw": resp.text if hasattr(resp, "text") else str(resp)}
        except Exception as e_http:
            # Network/request-level failure captured as response
            status = None
            resp_json = {"exception": str(e_http)}

        return {
            "id": rec_id,
            "request": request_obj,
            "status": status,
            "response": resp_json
        }

    except Exception as e_outer:
        # Per requirement: ignore all exceptions and return info for audit only
        return {
            "id": rec_id,
            "request": {},
            "status": None,
            "response": {"exception": str(e_outer)}
        }

# ----------------------------------------------------------------------
# Main entrypoint (handler)
# ----------------------------------------------------------------------
def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    # Hard-coded parallelism per your request
    MAX_WORKERS = 15

    # ------------------------------------------------------------------
    # Resolve DB and SCHEMA from control table
    # ------------------------------------------------------------------
    cfg_q = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB AS DBNAME,
               SALESFORCE_UPDATE_FROM_APP_SCHEMA AS SCHEMA_NAME
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
        WHERE SALESFORCE_UPDATE_OBJECT = 'ACCOUNT'
          AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
        LIMIT 1
    """
    cfg = session.sql(cfg_q).collect()
    if not cfg:
        raise Exception(f"Config not found for {SALESFORCE_UPDATE_FROM_APP}")

    app_db = cfg[0]["DBNAME"]
    app_schema = cfg[0]["SCHEMA_NAME"]

    # ------------------------------------------------------------------
    # Generate next LOAD_NBR
    # ------------------------------------------------------------------
    load_nbr = session.sql(
        f"SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR"
    ).collect()[0]["LOAD_NBR"]

    # ------------------------------------------------------------------
    # Determine previous successful run delta (baseline)
    # ------------------------------------------------------------------
    prev_run_q = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    prev_delta = session.sql(prev_run_q).collect()[0]["PREV_DELTA"]

    # ------------------------------------------------------------------
    # Compute current delta_end_date (max ROW_CRE_DT)
    # ------------------------------------------------------------------
    delta_end_row = session.sql(
        f"SELECT MAX(ROW_CRE_DT) AS DELTA_END FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE"
    ).collect()[0]
    delta_end = delta_end_row.get("DELTA_END")
    if delta_end is None:
        delta_end = session.sql("SELECT CURRENT_TIMESTAMP() AS TS").collect()[0]["TS"]

    # ------------------------------------------------------------------
    # Insert audit-control start row (LOAD_STATUS = 0 for start)
    # We will always close it to 1 at the end per your instruction.
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        SELECT {load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), 'SFADMIN'
    """).collect()

    # ------------------------------------------------------------------
    # Retrieve endpoint URL from param table
    # ------------------------------------------------------------------
    url_row = session.sql(f"""
        SELECT URL
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
        WHERE TARGET_TABLE_NAME = 'SALESFORCE_ACCOUNT_DATA_UPDATE'
        LIMIT 1
    """).collect()
    base_url = url_row[0]["URL"] if url_row else ""

    # ------------------------------------------------------------------
    # Load secrets and obtain Bearer token + subscription key (same as Contact proc)
    # ------------------------------------------------------------------
    # secret 'cred' is provided via SECRETS mapping on procedure
    try:
        cred_text = _snowflake.get_generic_secret_string('cred')
        cred = json.loads(cred_text) if cred_text else {}
    except Exception:
        cred = {}

    # subscription key if present in the secret
    subscriptionID = cred.get("subscriptionID")

    # Call token generation proc (same pattern used in Contact proc)
    try:
        auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
        auth_res = session.sql(auth_proc).collect()
        # Expecting the proc to return a column named SALESFORCE_AUTH_TOKEN_GEN or similar
        # Try multiple possible keys defensively
        if auth_res and len(auth_res) > 0:
            row0 = auth_res[0]
            # attempt to find token in returned row
            auth_token = None
            for k in row0.keys():
                if 'TOKEN' in k.upper() or 'SALESFORCE' in k.upper():
                    auth_token = row0[k]
                    break
        else:
            auth_token = None
    except Exception:
        auth_token = None

    # Prepare headers - include subscriptionID and bearer token if available
    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    # ------------------------------------------------------------------
    # Build list of records to process (based on row creation timestamp)
    # ------------------------------------------------------------------
    listing_q = f"""
        SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_delta}'
          AND ROW_CRE_DT <= '{delta_end}'
        ORDER BY ROW_CRE_DT
    """
    listing = session.sql(listing_q).collect()
    total = len(listing)

    # If no records, close audit control as success and exit
    if total == 0:
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end}',
                LOAD_END_DATE = CURRENT_TIMESTAMP()
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        return f"No account records to process. LOAD_NBR={load_nbr}"

    # ------------------------------------------------------------------
    # Process in parallel batches (MAX_WORKERS)
    # ------------------------------------------------------------------
    audit_entries = []
    batch_size = MAX_WORKERS

    for batch_num, start in enumerate(range(0, total, batch_size), start=1):
        batch_rows = listing[start:start+batch_size]
        print(f"Batch {batch_num} started — records {start+1} to {start+len(batch_rows)} (count={len(batch_rows)})")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for r in batch_rows:
                rec_id = r["SALESFORCE_ACCOUNT_DATA_UPDATE_ID"]
                futures.append(executor.submit(send_single, session, app_db, app_schema, rec_id, base_url, headers))

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    # Per requirement: ignore exceptions fully and capture minimal info
                    res = {"id": None, "request": {}, "status": None, "response": {"exception": str(e)}}
                audit_entries.append(res)

        print(f"Batch {batch_num} completed (processed {len(batch_rows)} records)")

    # ------------------------------------------------------------------
    # Bulk insert audit log entries
    # ------------------------------------------------------------------
    if audit_entries:
        values = []
        for a in audit_entries:
            recid = a.get("id")
            req = a.get("request", {})
            resp = a.get("response", {})
            try:
                req_text = json.dumps(req).replace("'", "''")
            except Exception:
                req_text = str(req).replace("'", "''")
            try:
                resp_text = json.dumps(resp).replace("'", "''")
            except Exception:
                resp_text = str(resp).replace("'", "''")
            status_val = a.get("status")
            values.append(f"('{recid}','{req_text}','{status_val}','{resp_text}',CURRENT_TIMESTAMP(),'SFADMIN')")

        if values:
            insert_stmt = f"""
                INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
                (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
                VALUES {','.join(values)}
            """
            session.sql(insert_stmt).collect()

    # ------------------------------------------------------------------
    # ALWAYS mark as success (LOAD_STATUS = 1) to avoid blocking future runs
    # ------------------------------------------------------------------
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end}',
            LOAD_END_DATE = CURRENT_TIMESTAMP()
        WHERE LOAD_NBR = {load_nbr}
    """).collect()

    return f"Account update completed successfully. LOAD_NBR={load_nbr} | RecordsProcessed={total}"

$$;
