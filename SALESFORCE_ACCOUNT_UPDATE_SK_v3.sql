USE ROLE ENTERPRISE_ADMIN_INTERNAL_ONSHORE;

CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_UPDATE(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pyjwt','cryptography','requests','simplejson','snowflake-snowpark-python','pandas')
SECRETS = ('cred' = UAT_DB_MANAGER.SECRETS.SALESFORCE_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
# Python Snowpark procedure
import simplejson as json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.snowpark as snowpark
import _snowflake

# ----------------------------------------------------------------------
# send_single: fetch payload for rec_id, call API (swallow ALL errors)
# ----------------------------------------------------------------------
def send_single(session, app_db, app_schema, rec_id, base_url, headers):
    try:
        # fetch row payload (exclude meta cols)
        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID = '{rec_id}'
        """
        rows = session.sql(payload_q).collect()
        if not rows:
            request_obj = {}
        else:
            rec_obj = rows[0]["RECORD_DATA"]
            # convert VARIANT -> dict safely
            if isinstance(rec_obj, dict):
                request_obj = rec_obj
            else:
                try:
                    request_obj = dict(rec_obj)
                except Exception:
                    try:
                        request_obj = json.loads(str(rec_obj))
                    except Exception:
                        request_obj = {}

        # call API using PUT (as per contact proc)
        try:
            resp = requests.put(base_url, headers=headers, json=request_obj, timeout=60)
            status = getattr(resp, "status_code", None)
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"raw": resp.text if hasattr(resp, "text") else str(resp)}
        except Exception as e_http:
            status = None
            resp_json = {"exception": str(e_http)}

        return {"id": rec_id, "request": request_obj, "status": status, "response": resp_json}

    except Exception as e_outer:
        # swallow any exception and return minimal info for audit
        return {"id": rec_id, "request": {}, "status": None, "response": {"exception": str(e_outer)}}


# ----------------------------------------------------------------------
# main handler
# ----------------------------------------------------------------------
def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    # Hard-coded parallelism per request
    MAX_WORKERS = 15

    # resolve app DB & schema from control table (same place contact proc uses)
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

    # generate load number
    load_nbr = session.sql(f"SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR").collect()[0]["LOAD_NBR"]

    # previous successful delta end
    prev_run_q = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    prev_delta = session.sql(prev_run_q).collect()[0]["PREV_DELTA"]

    # compute delta_end_date as max(row_cre_dt)
    delta_end_row = session.sql(f"SELECT MAX(ROW_CRE_DT) AS DELTA_END FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE").collect()[0]
    delta_end = delta_end_row.get("DELTA_END")
    if delta_end is None:
        delta_end = session.sql("SELECT CURRENT_TIMESTAMP() AS TS").collect()[0]["TS"]

    # insert audit-control start record (LOAD_STATUS=0 at start)
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        SELECT {load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), 'SFADMIN'
    """).collect()

    # endpoint lookup (same param table as contact)
    url_row = session.sql(f"""
        SELECT URL
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
        WHERE TARGET_TABLE_NAME = 'SALESFORCE_ACCOUNT_DATA_UPDATE'
        LIMIT 1
    """).collect()
    base_url = url_row[0]["URL"] if url_row else ""

    # load secret and subscriptionID 
    try:
        cred_text = _snowflake.get_generic_secret_string('cred')
        cred = json.loads(cred_text) if cred_text else {}
    except Exception:
        cred = {}
    subscriptionID = cred.get("subscriptionID")

    # generate token using existing auth proc 
    try:
        auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
        auth_row = session.sql(auth_proc).collect()
        if auth_row and len(auth_row) > 0:
            auth_token = auth_row[0].get(list(auth_row[0].keys())[0])
        else:
            auth_token = None
    except Exception:
        auth_token = None

    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    # fetch records to process (between prev_delta and delta_end)
    listing_q = f"""
        SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_delta}'
          AND ROW_CRE_DT <= '{delta_end}'
        ORDER BY ROW_CRE_DT
    """
    listing = session.sql(listing_q).collect()
    total = len(listing)

    if total == 0:
        # mark control complete
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end}',
                LOAD_END_DATE = CURRENT_TIMESTAMP()
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        return f"No account records to process. LOAD_NBR={load_nbr}"

    # process in batches of MAX_WORKERS, ignore all exceptions and responses
    audit_entries = []
    batch_size = MAX_WORKERS

    for batch_num, start in enumerate(range(0, total, batch_size), start=1):
        batch_rows = listing[start:start+batch_size]
        print(f"Batch {batch_num} started: records {start+1} to {start+len(batch_rows)} (count={len(batch_rows)})")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for r in batch_rows:
                rec_id = r["SALESFORCE_ACCOUNT_DATA_UPDATE_ID"]
                futures.append(executor.submit(send_single, session, app_db, app_schema, rec_id, base_url, headers))

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    # swallow exception and record minimal audit info
                    res = {"id": None, "request": {}, "status": None, "response": {"exception": str(e)}}
                audit_entries.append(res)

        print(f"Batch {batch_num} completed (processed {len(batch_rows)} records)")

    # bulk insert audit entries
    if audit_entries:
        vals = []
        for a in audit_entries:
            rid = a.get("id")
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
            vals.append(f"('{rid}','{req_text}','{status_val}','{resp_text}',CURRENT_TIMESTAMP(),'SFADMIN')")

        if vals:
            insert_stmt = f"""
                INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
                (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
                VALUES {','.join(vals)}
            """
            session.sql(insert_stmt).collect()

    # finally mark control as success ALWAYS (per your request)
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end}',
            LOAD_END_DATE = CURRENT_TIMESTAMP()
        WHERE LOAD_NBR = {load_nbr}
    """).collect()

    return f"Account update completed successfully. LOAD_NBR={load_nbr} | RecordsProcessed={total}"

$$;
