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
# Batch-enabled Contact Update procedure
import json
import time
import requests
import snowflake.snowpark as snowpark
import _snowflake
from concurrent.futures import ThreadPoolExecutor, as_completed


# ---------------------------
# Config
# ---------------------------
MAX_WORKERS = 15         # threads for inner parallelism
BIG_BATCH_SIZE = 2000    # process 2k records then refresh token + wait
COOLDOWN_SECONDS = 120   # wait between big batches
MAX_RETRIES = 3          # retries per request
RETRY_DELAYS = [1, 3, 6] # backoff seconds

# ---------------------------
# Token helper (calls your existing token proc)
# ---------------------------
def refresh_auth_token(session):
    try:
        # call token proc; adapt if your proc signature differs
        auth_res = session.sql("CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')").collect()
        if auth_res and len(auth_res) > 0:
            # return first column value
            return list(auth_res[0].values())[0]
    except Exception:
        return None
    return None

# ---------------------------
# Headers builder — uses the exact secret-loading method you requested
# ---------------------------
def build_headers(token):
    # load secret exactly as requested
    try:
        credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    except Exception:
        credentials = {}
    subscriptionID = credentials.get("subscriptionID")
    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers

# ---------------------------
# Send a single record with retry and transient handling
# ---------------------------
def send_one(session, app_db, app_schema, rec_id, base_url, token_getter):
    """
    token_getter: function returning current token (string)
    Returns audit dict: {id, request, status, response}
    """
    try:
        # fetch payload (exclude meta columns)
        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID = '{rec_id}'
        """
        rows = session.sql(payload_q).collect()
        if not rows:
            req_obj = {}
        else:
            rec_obj = rows[0]["RECORD_DATA"]
            try:
                req_obj = dict(rec_obj) if isinstance(rec_obj, dict) else json.loads(str(rec_obj))
            except Exception:
                try:
                    req_obj = json.loads(str(rec_obj).replace("'", '"'))
                except Exception:
                    req_obj = {}

        last_status = None
        last_resp = None

        for attempt in range(MAX_RETRIES):
            try:
                token = token_getter()
                headers = build_headers(token)
                resp = requests.put(base_url, headers=headers, json=req_obj, timeout=60)
                status = getattr(resp, "status_code", None)
                last_status = status
                try:
                    resp_json = resp.json()
                except Exception:
                    resp_json = {"raw": resp.text if hasattr(resp, "text") else str(resp)}
                last_resp = resp_json

                # If unauthorized, caller will refresh token between big batches;
                # but we will attempt immediate retry a few times here for transient issues
                if status == 401:
                    # short backoff then retry
                    time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                    continue

                if status in (429, 500, 502, 503, 504):
                    time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                    continue

                # final (success or non-retriable error)
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

    except Exception as e_outer:
        return {"id": rec_id, "request": {}, "status": None, "response": {"exception": str(e_outer)}}


# ---------------------------
# Main handler
# ---------------------------
def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    # Resolve app DB/schema from control table (same pattern as Contact proc)
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
        raise Exception(f"Config not found for {SALESFORCE_UPDATE_FROM_APP}")
    app_db = cfg[0]["DBNAME"]
    app_schema = cfg[0]["SCHEMA_NAME"]

    # Generate load number
    load_nbr = session.sql(f"SELECT {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR").collect()[0]["LOAD_NBR"]

    # previous successful delta
    prev_run_q = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    prev_delta = session.sql(prev_run_q).collect()[0]["PREV_DELTA"]

    # compute delta_end (max ROW_CRE_DT)
    delta_end_q = f"""
        SELECT MAX(ROW_CRE_DT) AS DELTA_END
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
    """
    delta_end_row = session.sql(delta_end_q).collect()[0]
    delta_end = delta_end_row["DELTA_END"]
    if delta_end is None:
        delta_end = session.sql("SELECT CURRENT_TIMESTAMP() AS TS").collect()[0]["TS"]

    # insert audit-control start (LOAD_STATUS = 0 initially)
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        VALUES ({load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), 'SFADMIN')
    """).collect()

    # Resolve API endpoint
    url_row = session.sql(f"""
        SELECT URL
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
        WHERE TARGET_TABLE_NAME = 'SALESFORCE_CONTACT_DATA_UPDATE'
        LIMIT 1
    """).collect()
    base_url = url_row[0]["URL"] if url_row else ""

    # Initial token and mutable holder for updates
    initial_token = refresh_auth_token(session)
    auth_token_holder = [initial_token]  # mutable holder so nested functions can see updates

    # token getter for per-request calls (reads current token from holder)
    def token_getter():
        return auth_token_holder[0]

    # helper to refresh token and update holder
    def refresh_and_update():
        new_token = refresh_auth_token(session)
        if new_token:
            auth_token_holder[0] = new_token
        return auth_token_holder[0]

    # Initial headers built via build_headers (will be refreshed per big-batch)
    headers = build_headers(auth_token_holder[0])

    # Build listing of IDs to process in this run (based on delta)
    listing_q = f"""
        SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_delta}'
          AND ROW_CRE_DT <= '{delta_end}'
        ORDER BY ROW_CRE_DT
    """
    listing_rows = session.sql(listing_q).collect()
    total = len(listing_rows)

    if total == 0:
        # mark audit-control success and return
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end}',
                LOAD_END_DATE = CURRENT_TIMESTAMP()
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        return f"No contact records to process. LOAD_NBR={load_nbr}"

    # convert listing to python list of ids
    id_list = [r["SALESFORCE_CONTACT_DATA_UPDATE_ID"] for r in listing_rows]

    # process in big batches
    for big_batch_num, start in enumerate(range(0, total, BIG_BATCH_SIZE), start=1):

        big_batch_ids = id_list[start:start + BIG_BATCH_SIZE]
        print(f"Starting BIG BATCH {big_batch_num}: records {start+1} to {start+len(big_batch_ids)} (count={len(big_batch_ids)})")

        # collect audit entries for this big batch only (flush after big batch)
        audit_entries = []

        # inside each big batch, process small sub-batches of MAX_WORKERS
        for small_start in range(0, len(big_batch_ids), MAX_WORKERS):
            small_ids = big_batch_ids[small_start: small_start + MAX_WORKERS]
            print(f"  Small sub-batch processing: {small_start+1} to {small_start+len(small_ids)} (count={len(small_ids)})")

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(send_one, session, app_db, app_schema, recid, base_url, token_getter): recid for recid in small_ids}
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                    except Exception as e:
                        res = {"id": futures[fut], "request": {}, "status": None, "response": {"exception": str(e)}}
                    audit_entries.append(res)

        # bulk insert audit_entries for this big batch
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
                    INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
                    (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
                    VALUES {','.join(values)}
                """
                session.sql(insert_stmt).collect()

        print(f"Completed BIG BATCH {big_batch_num} (processed {len(big_batch_ids)} records)")

        # refresh token for next big batch
        print("Refreshing auth token for next big batch...")
        refreshed_token = refresh_and_update()
        headers = build_headers(refreshed_token)

        # update progress in audit control (so control table shows activity)
        try:
            session.sql(f"""
                UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
                SET LOAD_END_DATE = CURRENT_TIMESTAMP()
                WHERE LOAD_NBR = {load_nbr}
            """).collect()
        except Exception:
            pass

        # cooldown before next big batch to avoid throttling
        print(f"Waiting {COOLDOWN_SECONDS} seconds before next big batch...")
        try:
            session.sql(f"CALL SYSTEM$WAIT({COOLDOWN_SECONDS})").collect()
        except Exception:
            time.sleep(COOLDOWN_SECONDS)

    # finalize audit-control as success
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end}',
            LOAD_END_DATE = CURRENT_TIMESTAMP()
        WHERE LOAD_NBR = {load_nbr}
    """).collect()

    return f"Contact update completed: LOAD_NBR={load_nbr} | RecordsProcessed={total}"

$$;
