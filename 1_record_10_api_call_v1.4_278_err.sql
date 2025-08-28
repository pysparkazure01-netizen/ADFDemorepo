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

    MAX_WORKERS = 10   # run 10 API calls in parallel
    RUN_ID = None
    LOAD_STATUS = 'SUCCESS'

    # --- Get DB & schema from control table ---
    app_db_schema_query = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA 
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL 
        WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT' 
        AND SALESFORCE_UPDATE_FROM_APP='{SALESFORCE_UPDATE_FROM_APP}'
    """
    app_db_return = session.sql(app_db_schema_query).collect()
    app_db = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_DB"]
    app_schema = app_db_return[0]["SALESFORCE_UPDATE_FROM_APP_SCHEMA"]

    # --- Get credentials ---
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    subscriptionID = credentials.get("subscriptionID")

    # --- Auth token ---
    auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
    auth_token = session.sql(auth_proc).collect()[0]["SALESFORCE_AUTH_TOKEN_GEN"]

    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionID,
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }

    # --- Get API URL ---
    url_query = f"""
        SELECT url AS URL 
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS 
        WHERE TARGET_TABLE_NAME='SALESFORCE_CONTACT_DATA_UPDATE'
    """
    url_link = session.sql(url_query).collect()
    base_url = url_link[0]["URL"]

    # --- Get delta boundary (prev run end date) ---
    prev_delta_query = f"""
        SELECT COALESCE(MAX(DELTA_END_DATE), '1900-01-01'::timestamp) AS PREV_RUN_DELTA
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        WHERE SALESFORCE_UPDATE_OBJECT='CONTACT' AND LOAD_STATUS='SUCCESS'
    """
    prev_delta = session.sql(prev_delta_query).collect()[0]["PREV_RUN_DELTA"]

    # --- Current run boundary ---
    curr_delta_query = "SELECT CURRENT_TIMESTAMP AS CURR_RUN_DELTA"
    curr_delta = session.sql(curr_delta_query).collect()[0]["CURR_RUN_DELTA"]

    # --- Insert audit control start record ---
    insert_audit_control = f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        (SALESFORCE_UPDATE_OBJECT, SALESFORCE_UPDATE_FROM_APP, DELTA_START_DATE, DELTA_END_DATE, LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        VALUES ('CONTACT', '{SALESFORCE_UPDATE_FROM_APP}', '{prev_delta}', '{curr_delta}', 'STARTED', CURRENT_TIMESTAMP, 'SFADMIN')
    """
    session.sql(insert_audit_control).collect()

    # Get RUN_ID for update later
    run_id_query = f"""
        SELECT MAX(SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL_ID) AS RUN_ID
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        WHERE SALESFORCE_UPDATE_OBJECT='CONTACT'
        AND SALESFORCE_UPDATE_FROM_APP='{SALESFORCE_UPDATE_FROM_APP}'
    """
    RUN_ID = session.sql(run_id_query).collect()[0]["RUN_ID"]

    # --- Fetch only new records since last run ---
    contact_listing_query = f"""
        SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID, CONTACTID
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{prev_delta}'
        AND ROW_CRE_DT <= '{curr_delta}'
    """
    contact_listing = session.sql(contact_listing_query).collect()

    if not contact_listing:
        update_control = f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS='NO_DATA', DELTA_END_DATE='{curr_delta}'
            WHERE SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL_ID={RUN_ID}
        """
        session.sql(update_control).collect()
        return "No new records found"

    # --- Function to send 1 record ---
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

            # ✅ PUT single dict payload
            resp = requests.put(base_url, headers=headers, json=record)

            return {
                "id": SALESFORCE_CONTACT_DATA_UPDATE_ID,
                "request": record,
                "status": resp.status_code,
                "response": resp.text
            }
        return None

    # --- Run ALL records with 10 parallel workers ---
    results = []
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(send_record, rec) for rec in contact_listing]
            for future in as_completed(futures):
                res = future.result()
                if res:
                    results.append(res)
    except Exception as e:
        LOAD_STATUS = 'FAILED'
        raise e

    # --- Insert logs for all processed records ---
    values = []
    for r in results:
        request_json = json.dumps(r["request"]).replace("'", "''")
        response_json = r["response"].replace("'", "''")
        values.append(f"""(
            '{r["id"]}',
            '{request_json}',
            '{r["status"]}',
            '{response_json}',
            CURRENT_TIMESTAMP,
            'SFADMIN'
        )""")

    if values:
        insert_query = f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES {",".join(values)}
        """
        session.sql(insert_query).collect()

    # --- Update audit control table with status ---
    update_control = f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS='{LOAD_STATUS}', DELTA_END_DATE='{curr_delta}'
        WHERE SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL_ID={RUN_ID}
    """
    session.sql(update_control).collect()

    return f"Processed {len(results)} records with {MAX_WORKERS} parallel API workers | RunID={RUN_ID}, Status={LOAD_STATUS}"
$$;
