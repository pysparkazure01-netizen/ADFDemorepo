CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE(
    SALESFORCE_UPDATE_FROM_APP VARCHAR
)
RETURNS string
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

def chunk_list(lst, size):
    """Split a list into chunks of given size."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def salesforce_contact_update_process(session: snowpark.Session, app_db, app_schema, load_nbr):
    # Default chunk size
    chunk_size = 200

    # Get credentials
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    subscriptionID = credentials.get("subscriptionID")

    # Get previous run delta
    prev_run_delta_query = f"""SELECT NVL(MAX(delta_end_date), '1990-01-01') AS previous_run_delta_end_date 
                               FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL 
                               WHERE load_status = 1"""
    previous_run_delta_end_date = session.sql(prev_run_delta_query).collect()[0]["PREVIOUS_RUN_DELTA_END_DATE"]

    # Auth token
    auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
    auth_token = session.sql(auth_proc).collect()[0]["SALESFORCE_AUTH_TOKEN_GEN"]

    # Insert control start
    cntrl_table_query = f"""
    INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL 
    SELECT {load_nbr}, '{previous_run_delta_end_date}', NULL, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP, 'SFADMIN'
    """
    session.sql(cntrl_table_query).collect()

    # Headers
    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionID,
        'Authorization': f'Bearer {auth_token}'
    }

    # Collect all contacts needing update
    contact_listing_data_query = f"""
    SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID
    FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
    WHERE row_cre_dt > '{previous_run_delta_end_date}'
    """
    contact_listing = session.sql(contact_listing_data_query).collect()

    # Salesforce URL
    target_table_name = 'SALESFORCE_CONTACT_DATA_UPDATE'
    url_query = f"""SELECT url AS URL 
                    FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS 
                    WHERE TARGET_TABLE_NAME='{target_table_name}'"""
    url_link = session.sql(url_query).collect()
    salesforce_contact_update_url = [row["URL"] for row in url_link][0]

    # Build payload
    records = []
    for sf_contact_listing in contact_listing:
        SALESFORCE_CONTACT_DATA_UPDATE_ID = sf_contact_listing["SALESFORCE_CONTACT_DATA_UPDATE_ID"]
        upload_data_query = f"""
        SELECT ARRAY_AGG(
            OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID))
        ) AS JSON_OUTPUT
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID='{SALESFORCE_CONTACT_DATA_UPDATE_ID}'
        """
        contact_data_update = session.sql(upload_data_query).collect()[0]["JSON_OUTPUT"]
        if contact_data_update:
            records.extend(json.loads(contact_data_update))

    # Process in chunks of 200
    if records:
        for batch in chunk_list(records, chunk_size):
            response = requests.put(salesforce_contact_update_url, headers=headers, json=batch)
            response_text = response.text
            response_status = response.status_code

            # Escape JSON/response safely for SQL
            request_json = json.dumps(batch).replace("'", "''")
            response_json = response_text.replace("'", "''")

            sf_contact_query = f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
            SELECT NULL AS SALESFORCE_CONTACT_DATA_UPDATE_ID,
                   '{request_json}' AS REQUEST_JSON,
                   '{response_status}' AS RESPONSE_STATUS,
                   '{response_json}' AS RESPONSE_JSON,
                   CURRENT_TIMESTAMP AS ROW_CRE_DT,
                   'SFADMIN' AS ROW_CRE_USR_ID
            """
            session.sql(sf_contact_query).collect()
    else:
        # Log empty run
        sf_contact_query = f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
        SELECT NULL, '[]', '204', 'No records to update', CURRENT_TIMESTAMP, 'SFADMIN'
        """
        session.sql(sf_contact_query).collect()

def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):
    # Get DB & schema
    app_db_schema_query = f"""SELECT SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA 
                              FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL 
                              WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT' 
                              AND SALESFORCE_UPDATE_FROM_APP='{SALESFORCE_UPDATE_FROM_APP}'"""
    app_db_return = session.sql(app_db_schema_query).collect()
    app_db = [row["SALESFORCE_UPDATE_FROM_APP_DB"] for row in app_db_return][0]
    app_schema = [row["SALESFORCE_UPDATE_FROM_APP_SCHEMA"] for row in app_db_return][0]

    # Check load status
    load_status_query = f"""SELECT NVL(MIN(LOAD_STATUS),1) AS LOAD_STATUS 
                            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL"""
    load_status = session.sql(load_status_query).collect()[0]["LOAD_STATUS"]

    if load_status == 0:
        return f"Previous load is in progress"
    else:
        # Load number
        load_nbr_query = f"""SELECT {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_LOAD_NBR.nextval AS LOAD_NBR"""
        load_nbr = session.sql(load_nbr_query).collect()[0]["LOAD_NBR"]

        # Delta end date
        delta_end_date_query = f"""SELECT MAX(ROW_CRE_DT) AS delta_end_date 
                                   FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE"""
        delta_end_date = session.sql(delta_end_date_query).collect()[0]["DELTA_END_DATE"]

        # Run update process
        salesforce_contact_update_process(session, app_db, app_schema, load_nbr)

        # Update control table
        cntrl_table_update_query = f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS=1, DELTA_END_DATE = '{delta_end_date}', LOAD_END_DATE=GETDATE()
        WHERE LOAD_NBR={load_nbr}
        """
        session.sql(cntrl_table_update_query).collect()

        return f"Salesforce Contact Update Batch Completed Successfully (Chunk Size=200)"
$$;
