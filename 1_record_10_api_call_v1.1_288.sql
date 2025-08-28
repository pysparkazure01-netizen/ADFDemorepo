CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE(APP_NAME STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','requests')
HANDLER = 'main'
AS
$$
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 10   # parallel API calls per batch

def main(session, APP_NAME):

    app_db = "UAT_ADS"
    app_schema = "DWNSTRM_SALESFORCE"

    # ------------------------------------------------------------------
    # 1) Get Salesforce credentials
    # ------------------------------------------------------------------
    cred_query = f"""
        SELECT LOGIN_URL, ACCESS_TOKEN, INSTANCE_URL, OBJECT_API_NAME
        FROM {app_db}.{app_schema}.SALESFORCE_CREDENTIALS
        WHERE APP_NAME = '{APP_NAME}'
    """
    creds = session.sql(cred_query).collect()[0]
    base_url = f"{creds['INSTANCE_URL']}/services/data/v57.0/sobjects/{creds['OBJECT_API_NAME']}/"
    headers = {"Authorization": f"Bearer {creds['ACCESS_TOKEN']}",
               "Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # 2) Get last successful delta timestamp
    # ------------------------------------------------------------------
    prev_delta_query = f"""
        SELECT COALESCE(MAX(END_TS), '1900-01-01'::timestamp) AS PREV_RUN_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        WHERE STATUS = 'SUCCESS'
    """
    prev_delta = session.sql(prev_delta_query).collect()[0]["PREV_RUN_DELTA"]

    # ------------------------------------------------------------------
    # 3) Insert audit control entry (STARTED)
    # ------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        (APP_NAME, START_TS, STATUS)
        VALUES ('{APP_NAME}', CURRENT_TIMESTAMP, 'STARTED')
    """).collect()

    # ------------------------------------------------------------------
    # 4) Fetch records greater than prev delta
    # ------------------------------------------------------------------
    contact_query = f"""
        SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID, CONTACTID
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_delta}'
    """
    contact_listing = session.sql(contact_query).collect()

    if not contact_listing:
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            SET END_TS = CURRENT_TIMESTAMP, STATUS = 'NO_DATA'
            WHERE APP_NAME = '{APP_NAME}' AND STATUS='STARTED'
        """).collect()
        return "No new records to process"

    # ------------------------------------------------------------------
    # 5) Define record sender
    # ------------------------------------------------------------------
    def send_record(sf_contact_listing):
        SALESFORCE_CONTACT_DATA_UPDATE_ID = sf_contact_listing["SALESFORCE_CONTACT_DATA_UPDATE_ID"]
        CONTACTID = sf_contact_listing["CONTACTID"]

        upload_data_query = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID)) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID='{SALESFORCE_CONTACT_DATA_UPDATE_ID}'
        """
        contact_data_update = session.sql(upload_data_query).collect()[0]["RECORD_DATA"]

        if contact_data_update:
            if isinstance(contact_data_update, str):
                record = json.loads(contact_data_update)
            else:
                record = dict(contact_data_update)

            record["CONTACTID"] = CONTACTID

            resp = requests.put(base_url + CONTACTID, headers=headers, json=record)

            return {
                "id": SALESFORCE_CONTACT_DATA_UPDATE_ID,
                "request": record,
                "status": resp.status_code,
                "response": resp.text
            }
        return None

    # ------------------------------------------------------------------
    # 6) Dispatch in parallel with batch messages
    # ------------------------------------------------------------------
    results = []
    batch_size = MAX_WORKERS
    total_records = len(contact_listing)

    for batch_num, i in enumerate(range(0, total_records, batch_size), start=1):
        batch = contact_listing[i:i+batch_size]

        print(f"Batch {batch_num} started (records {i+1}–{i+len(batch)})")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(send_record, r) for r in batch]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    results.append(res)

        print(f"Batch {batch_num} completed")

    # ------------------------------------------------------------------
    # 7) Write API logs
    # ------------------------------------------------------------------
    for res in results:
        session.sql(f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 'SFADMIN')
        """, (res["id"], json.dumps(res["request"]), str(res["status"]), res["response"])).collect()

    # ------------------------------------------------------------------
    # 8) Mark audit control SUCCESS
    # ------------------------------------------------------------------
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SET END_TS = CURRENT_TIMESTAMP, STATUS = 'SUCCESS'
        WHERE APP_NAME = '{APP_NAME}' AND STATUS='STARTED'
    """).collect()

    return f"Processed {len(results)} records in { (total_records+MAX_WORKERS-1)//MAX_WORKERS } batches"
$$;
