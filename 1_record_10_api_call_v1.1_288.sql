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
import _snowflake
import simplejson as json
import requests
import snowflake.snowpark as snowpark
from concurrent.futures import ThreadPoolExecutor, as_completed

def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    MAX_WORKERS = 10   # number of parallel API calls
    # -----------------------------------------------------------------
    # 1) Resolve app DB & schema (same logic as working proc)
    # -----------------------------------------------------------------
    app_db_schema_query = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
        WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT'
          AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
    """
    app_db_return = session.sql(app_db_schema_query).collect()
    if not app_db_return:
        return f"Config not found for {SALESFORCE_UPDATE_FROM_APP}"
    app_db = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_DB"]
    app_schema = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_SCHEMA"]

    # -----------------------------------------------------------------
    # 2) Prevent concurrent runs (LOAD_STATUS = 0 means in-progress)
    # -----------------------------------------------------------------
    load_status_query = f"""
        SELECT NVL(MIN(LOAD_STATUS), 1) AS LOAD_STATUS
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
    """
    load_status = session.sql(load_status_query).collect()[0]["LOAD_STATUS"]
    if load_status == 0:
        return "Previous load is in progress"

    # -----------------------------------------------------------------
    # 3) Get new LOAD_NBR and delta boundaries
    # -----------------------------------------------------------------
    load_nbr_query = f"SELECT {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS LOAD_NBR"
    load_nbr = session.sql(load_nbr_query).collect()[0]["LOAD_NBR"]

    prev_run_delta_query = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREVIOUS_RUN_DELTA_END_DATE
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    previous_run_delta_end_date = session.sql(prev_run_delta_query).collect()[0]["PREVIOUS_RUN_DELTA_END_DATE"]

    delta_end_date_query = f"""SELECT MAX(ROW_CRE_DT) AS DELTA_END_DATE
                               FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE"""
    delta_end_date = session.sql(delta_end_date_query).collect()[0]["DELTA_END_DATE"]

    # If no rows in source, delta_end_date can be None -> set to current timestamp
    if delta_end_date is None:
        delta_end_date = session.sql("SELECT CURRENT_TIMESTAMP AS NOW_TS").collect()[0]["NOW_TS"]

    # -----------------------------------------------------------------
    # 4) Insert audit-control start row (same column order as working proc)
    #    (LOAD_NBR, DELTA_START_DATE, DELTA_END_DATE, LOAD_START_DATE,
    #     LOAD_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
    # -----------------------------------------------------------------
    cntrl_table_insert = f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SELECT {load_nbr}, '{previous_run_delta_end_date}', NULL, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP, 'SFADMIN'
    """
    session.sql(cntrl_table_insert).collect()

    # -----------------------------------------------------------------
    # 5) Prepare credentials / token / headers / endpoint
    #    (we expect SALESFORCE_PARAM_API_ENDPOINTS to already contain the Contact endpoint)
    # -----------------------------------------------------------------
    # read subscription/token from secret
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

    # get endpoint (must be the Contact sObject endpoint or your middleware Contact endpoint)
    url_query = f"""SELECT url AS URL
                    FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
                    WHERE TARGET_TABLE_NAME='SALESFORCE_CONTACT_DATA_UPDATE'"""
    url_link = session.sql(url_query).collect()
    if not url_link:
        # no configured endpoint
        # leave control row as in-progress (LOAD_STATUS=0) so ops can investigate
        return "No endpoint configured in SALESFORCE_PARAM_API_ENDPOINTS for CONTACT"
    base_url = url_link[0]["URL"]

    # -----------------------------------------------------------------
    # 6) Fetch only new records between previous delta and current delta
    # -----------------------------------------------------------------
    contact_listing_query = f"""
        SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID, CONTACTID
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{previous_run_delta_end_date}'
          AND ROW_CRE_DT <= '{delta_end_date}'
        ORDER BY ROW_CRE_DT
    """
    contact_listing = session.sql(contact_listing_query).collect()

    if not contact_listing:
        # Update control to mark completed (no data)
        cntrl_update_no_data = f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end_date}',
                LOAD_END_DATE = GETDATE()
            WHERE LOAD_NBR = {load_nbr}
        """
        session.sql(cntrl_update_no_data).collect()
        return "No new records found"

    # -----------------------------------------------------------------
    # Helper: build and send a single record (PUT)
    # -----------------------------------------------------------------
    def send_record(row):
        rec_id = row["SALESFORCE_CONTACT_DATA_UPDATE_ID"]
        contact_id = row["CONTACTID"]

        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID = '{rec_id}'
        """
        rec_obj = session.sql(payload_q).collect()[0]["RECORD_DATA"]

        # Robust handling: rec_obj may be VARIANT (dict-like) or string
        if isinstance(rec_obj, str):
            record = json.loads(rec_obj)
        else:
            record = dict(rec_obj)

        # Ensure CONTACTID included in payload (your middleware/endpoint expects it)
        record["CONTACTID"] = contact_id

        # Send single-record payload using PUT (same as your working proc)
        resp = requests.put(base_url, headers=headers, json=record)

        return {
            "id": rec_id,
            "request": record,
            "status": resp.status_code,
            "response": resp.text
        }

    # -----------------------------------------------------------------
    # 7) Dispatch records in batches of MAX_WORKERS with prints
    # -----------------------------------------------------------------
    results = []
    total_records = len(contact_listing)
    batch_size = MAX_WORKERS

    for batch_num, i in enumerate(range(0, total_records, batch_size), start=1):
        batch = contact_listing[i:i+batch_size]

        # progress print (visible in proc logs / task logs)
        print(f"Batch {batch_num} started (records {i+1}–{i+len(batch)})")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(send_record, r) for r in batch]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    results.append(res)

        print(f"Batch {batch_num} completed")

    # -----------------------------------------------------------------
    # 8) Insert audit log rows (batch insert)
    # -----------------------------------------------------------------
    if results:
        values = []
        for r in results:
            req_json = json.dumps(r["request"]).replace("'", "''")
            resp_json = (r["response"] or "").replace("'", "''")
            values.append(f"""(
                '{r["id"]}',
                '{req_json}',
                '{r["status"]}',
                '{resp_json}',
                CURRENT_TIMESTAMP,
                'SFADMIN'
            )""")

        insert_log = f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES {','.join(values)}
        """
        session.sql(insert_log).collect()

    # -----------------------------------------------------------------
    # 9) Mark control row COMPLETE (LOAD_STATUS = 1)
    # -----------------------------------------------------------------
    cntrl_table_update = f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end_date}',
            LOAD_END_DATE = GETDATE()
        WHERE LOAD_NBR = {load_nbr}
    """
    session.sql(cntrl_table_update).collect()

    return f"Salesforce Contact Update Completed | Records={len(results)} | LOAD_NBR={load_nbr}"
$$;
