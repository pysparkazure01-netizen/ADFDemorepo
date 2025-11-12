CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_UPDATE_SK(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pyjwt','cryptography','requests','simplejson','snowflake-snowpark-python','pandas')
SECRETS = ('cred' = UAT_DB_MANAGER.SECRETS.SALESFORCE_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
import _snowflake
import simplejson as json
import requests
import snowflake.snowpark as snowpark
from concurrent.futures import ThreadPoolExecutor, as_completed

def salesforce_account_update_process(session: snowpark.Session, app_db: str, app_schema: str, load_nbr: int, prev_run_delta_end_date: str, delta_end_date: str, MAX_WORKERS: int):
    # Prepare credentials / token / headers / endpoint
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    subscriptionID = credentials.get("subscriptionID")

    # get auth token using your auth proc
    auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
    auth_token = session.sql(auth_proc).collect()[0]["SALESFORCE_AUTH_TOKEN_GEN"]

    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionID,
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }

    # get endpoint (Account endpoint or middleware URL)
    url_query = f"""SELECT url AS URL
                    FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
                    WHERE TARGET_TABLE_NAME='SALESFORCE_ACCOUNT_DATA_UPDATE'"""
    url_link = session.sql(url_query).collect()
    if not url_link:
        raise Exception("No endpoint configured in SALESFORCE_PARAM_API_ENDPOINTS for ACCOUNT")

    base_url = url_link[0]["URL"]

    # Fetch account listing (new records between prev delta and current delta)
    account_listing_query = f"""
        SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID, SALESFORCEACCOUNTID
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_run_delta_end_date}'
          AND ROW_CRE_DT <= '{delta_end_date}'
        ORDER BY ROW_CRE_DT
    """
    account_listing = session.sql(account_listing_query).collect()

    if not account_listing:
        return {"results": [], "failed": False}

    # helper: send a single record (PUT)
    def send_record(row):
        rec_id = row["SALESFORCE_ACCOUNT_DATA_UPDATE_ID"]
        account_id = row["SALESFORCEACCOUNTID"]

        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID = '{rec_id}'
        """
        rec_obj = session.sql(payload_q).collect()[0]["RECORD_DATA"]

        # Robust handling: rec_obj may be VARIANT (dict-like) or string
        if isinstance(rec_obj, str):
            record = json.loads(rec_obj)
        else:
            record = dict(rec_obj)

        # Ensure SALESFORCEACCOUNTID included if needed by endpoint
        record["ACCOUNTID"] = account_id

        # Send single-record payload using PUT (or change to POST if endpoint expects)
        resp = requests.put(base_url, headers=headers, json=record)

        # Try to parse JSON response safely
        resp_text = resp.text or ""
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {"raw": resp_text}

        return {
            "id": rec_id,
            "request": record,
            "status": resp.status_code,
            "response": resp_json
        }

    # run in batches of MAX_WORKERS
    results = []
    failed_any = False
    total_records = len(account_listing)
    batch_size = MAX_WORKERS

    for batch_num, i in enumerate(range(0, total_records, batch_size), start=1):
        batch = account_listing[i:i+batch_size]
        print(f"Batch {batch_num} started (records {i+1}–{i+len(batch)})")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(send_record, r) for r in batch]
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                except Exception as e:
                    # Record a failed synthetic response
                    failed_any = True
                    results.append({
                        "id": "unknown",
                        "request": {},
                        "status": 0,
                        "response": {"error": str(e)}
                    })
                else:
                    # If API returned business-level "success": false, treat as failure
                    status = res.get("status")
                    resp_json = res.get("response", {})
                    success_flag = False
                    if isinstance(resp_json, dict):
                        # If Salesforce style response contains 'success' boolean
                        success_flag = resp_json.get("success") is True
                    if status != 200 or (isinstance(resp_json, dict) and not success_flag and status==200):
                        failed_any = True
                    results.append(res)

        print(f"Batch {batch_num} completed")

    return {"results": results, "failed": failed_any}


def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):
    MAX_WORKERS = 15   # tuneable: start at 10–15
    # Resolve app DB & schema
    app_db_schema_query = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
        WHERE SALESFORCE_UPDATE_OBJECT = 'ACCOUNT'
          AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
    """
    app_db_return = session.sql(app_db_schema_query).collect()
    if not app_db_return:
        raise Exception(f"Config not found for {SALESFORCE_UPDATE_FROM_APP}")
    app_db = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_DB"]
    app_schema = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_SCHEMA"]

    # Prevent concurrent runs
    load_status_query = f"SELECT NVL(MIN(LOAD_STATUS),1) AS LOAD_STATUS FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL"
    load_status = session.sql(load_status_query).collect()[0]["LOAD_STATUS"]
    if load_status == 0:
        return "Previous load is in progress"

    # Generate load number
    load_nbr_query = f"SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR"
    load_nbr = session.sql(load_nbr_query).collect()[0]["LOAD_NBR"]

    # Get previous successful run delta end
    prev_run_delta_query = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREVIOUS_RUN_DELTA_END_DATE
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    previous_run_delta_end_date = session.sql(prev_run_delta_query).collect()[0]["PREVIOUS_RUN_DELTA_END_DATE"]

    # Compute delta_end_date (max row_cre_dt)
    delta_end_date_query = f"SELECT MAX(ROW_CRE_DT) AS DELTA_END_DATE FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE"
    delta_end_date = session.sql(delta_end_date_query).collect()[0]["DELTA_END_DATE"]
    if delta_end_date is None:
        delta_end_date = session.sql("SELECT CURRENT_TIMESTAMP AS NOW_TS").collect()[0]["NOW_TS"]

    # Insert audit-control start row (LOAD_STATUS=0)
    cntrl_table_insert = f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SELECT {load_nbr}, '{previous_run_delta_end_date}', NULL, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP, 'SFADMIN'
    """
    session.sql(cntrl_table_insert).collect()

    # Run account update process (parallel)
    try:
        proc_result = salesforce_account_update_process(session, app_db, app_schema, load_nbr, previous_run_delta_end_date, delta_end_date, MAX_WORKERS)
    except Exception as ex:
        # Update control row as failed with error and re-raise to make ADF fail
        err_msg = str(ex).replace("'", "''")
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 0,
                LOAD_END_DATE = CURRENT_TIMESTAMP,
                ROW_CRE_DT = CURRENT_TIMESTAMP
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        raise

    results = proc_result.get("results", [])
    failed_any = proc_result.get("failed", False)

    # Insert audit log rows in batch (if results exist)
    if results:
        values = []
        for r in results:
            req_json = json.dumps(r.get("request", {})).replace("'", "''")
            try:
                resp_json_text = json.dumps(r.get("response", {}))
            except Exception:
                resp_json_text = str(r.get("response", "")).replace("'", "''")
            resp_json_text = resp_json_text.replace("'", "''")
            values.append(f"""(
                '{r.get("id")}',
                '{req_json}',
                '{r.get("status")}',
                '{resp_json_text}',
                CURRENT_TIMESTAMP,
                'SFADMIN'
            )""")
        insert_log = f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES {','.join(values)}
        """
        session.sql(insert_log).collect()

    # If any failures flagged, update control row to failed and raise to ADF
    if failed_any:
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 0,
                LOAD_END_DATE = CURRENT_TIMESTAMP
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        raise Exception(f"One or more account API calls failed in LOAD_NBR={load_nbr}. Check audit log for details.")

    # Otherwise mark control row complete (LOAD_STATUS=1)
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end_date}',
            LOAD_END_DATE = CURRENT_TIMESTAMP
        WHERE LOAD_NBR = {load_nbr}
    """).collect()

    return f"Salesforce Account Update Completed | Records={len(results)} | LOAD_NBR={load_nbr}"
$$;
