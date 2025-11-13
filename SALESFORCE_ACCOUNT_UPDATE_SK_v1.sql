CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_UPDATE(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'main'
EXECUTE AS OWNER
AS
$$
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import snowflake.snowpark as snowpark
import _snowflake

# ----------------------------------------------------------------------
# Send a single API request (ignore all failures)
# ----------------------------------------------------------------------
def send_single(session, app_db, app_schema, rec_id, base_url, headers):
    try:
        # Fetch payload for the record
        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID = '{rec_id}'
        """
        rec_row = session.sql(payload_q).collect()

        if not rec_row:
            request_obj = {}
        else:
            rec_obj = rec_row[0]["RECORD_DATA"]

            # Convert VARIANT to dict safely
            if isinstance(rec_obj, dict):
                request_obj = rec_obj
            else:
                try:
                    request_obj = dict(rec_obj)
                except:
                    request_obj = json.loads(str(rec_obj))

        # Try to call API (ignore any errors)
        try:
            resp = requests.put(base_url, headers=headers, json=request_obj, timeout=60)
            status = getattr(resp, "status_code", None)

            try:
                resp_json = resp.json()
            except:
                resp_json = {"raw": resp.text if hasattr(resp, "text") else str(resp)}

        except Exception as e_http:
            status = None
            resp_json = {"exception": str(e_http)}

        return {
            "id": rec_id,
            "request": request_obj,
            "status": status,
            "response": resp_json
        }

    except Exception as e:
        # Ignore all exceptions completely
        return {"id": rec_id, "request": {}, "status": None, "response": {"exception": str(e)}}

# ----------------------------------------------------------------------
# Main Snowflake procedure handler
# ----------------------------------------------------------------------
def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    # Parallel worker count (hard-coded)
    MAX_WORKERS = 15

    # ------------------------------------------------------------------
    # Read config from control table
    # ------------------------------------------------------------------
    cfg_q = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB AS DBNAME,
               SALESFORCE_UPDATE_FROM_APP_SCHEMA AS SCHEMA_NAME
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
        WHERE SALESFORCE_UPDATE_OBJECT = 'ACCOUNT'
          AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
    """
    cfg = session.sql(cfg_q).collect()

    if not cfg:
        raise Exception(f"No config found for {SALESFORCE_UPDATE_FROM_APP}")

    app_db = cfg[0]["DBNAME"]
    app_schema = cfg[0]["SCHEMA_NAME"]

    # ------------------------------------------------------------------
    # Generate LOAD_NBR
    # ------------------------------------------------------------------
    load_nbr = session.sql(
        f"SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR"
    ).collect()[0]["LOAD_NBR"]

    # ------------------------------------------------------------------
    # Get previous successful delta timestamp
    # ------------------------------------------------------------------
    prev_run_q = f"""
        SELECT NVL(MAX(DELTA_END_DATE),'1990-01-01') AS PREV_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    prev_delta = session.sql(prev_run_q).collect()[0]["PREV_DELTA"]

    # ------------------------------------------------------------------
    # Compute new delta_end_date
    # ------------------------------------------------------------------
    delta_end_row = session.sql(
        f"SELECT MAX(ROW_CRE_DT) AS DELTA_END FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE"
    ).collect()[0]

    delta_end = delta_end_row.get("DELTA_END")

    if delta_end is None:
        delta_end = session.sql("SELECT CURRENT_TIMESTAMP() AS TS").collect()[0]["TS"]

    # ------------------------------------------------------------------
    # Start audit control entry
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        SELECT {load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL, 0, CURRENT_TIMESTAMP(), 'SFADMIN'
    """).collect()

    # ------------------------------------------------------------------
    # Fetch endpoint URL
    # ------------------------------------------------------------------
    url_row = session.sql(f"""
        SELECT URL
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
        WHERE TARGET_TABLE_NAME = 'SALESFORCE_ACCOUNT_DATA_UPDATE'
        LIMIT 1
    """).collect()

    base_url = url_row[0]["URL"] if url_row else ""

    # Simple headers (adjust as needed)
    headers = {"Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # Fetch records to update
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

    if total == 0:
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end}',
                LOAD_END_DATE = CURRENT_TIMESTAMP()
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        return f"No account records to process. LOAD_NBR={load_nbr}"

    audit_entries = []
    batch_size = MAX_WORKERS

    # ------------------------------------------------------------------
    # Parallel batch processing
    # ------------------------------------------------------------------
    for batch_num, start in enumerate(range(0, total, batch_size), start=1):

        batch_rows = listing[start:start+batch_size]

        print(f"Starting batch {batch_num}, records {start+1} to {start+len(batch_rows)}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            for r in batch_rows:
                rec_id = r["SALESFORCE_ACCOUNT_DATA_UPDATE_ID"]
                futures.append(executor.submit(
                    send_single, session, app_db, app_schema, rec_id, base_url, headers
                ))

            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    res = {
                        "id": None,
                        "request": {},
                        "status": None,
                        "response": {"exception": str(e)}
                    }
                audit_entries.append(res)

        print(f"Completed batch {batch_num}")

    # ------------------------------------------------------------------
    # Insert audit log in bulk
    # ------------------------------------------------------------------
    if audit_entries:
        values = []
        for a in audit_entries:
            recid = a.get("id")
            req = a.get("request", {})
            resp = a.get("response", {})

            req_text = json.dumps(req).replace("'", "''")
            resp_text = json.dumps(resp).replace("'", "''")

            status_val = a.get("status")

            values.append(
                f"('{recid}','{req_text}','{status_val}','{resp_text}',CURRENT_TIMESTAMP(),'SFADMIN')"
            )

        session.sql(f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES {",".join(values)}
        """).collect()

    # ------------------------------------------------------------------
    # ALWAYS mark load as SUCCESS (as per your requirement)
    # ------------------------------------------------------------------
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end}',
            LOAD_END_DATE = CURRENT_TIMESTAMP()
        WHERE LOAD_NBR = {load_nbr}
    """).collect()

    return f"Account update completed successfully. LOAD_NBR={load_nbr}, Records={total}"

$$;
