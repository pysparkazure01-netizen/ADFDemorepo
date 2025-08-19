CREATE OR REPLACE PROCEDURE PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE_BATCH(
    SALESFORCE_UPDATE_FROM_APP VARCHAR,
    BATCH_NUMBER INT,
    BATCH_SIZE INT
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (SALESFORCE_API_EXTERNAL_ACCESS_INTEGRATION)
PACKAGES = ('pyjwt', 'cryptography', 'requests', 'simplejson', 'snowflake-snowpark-python', 'pandas')
SECRETS = ('cred' = PROD_DB_MANAGER.SECRETS.SALESFORCE_API_INTEGRATION_SECRET)
EXECUTE AS OWNER
AS
$$
import _snowflake
import simplejson as json
import requests
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def salesforce_contact_update_process_batch(session: snowpark.Session, app_db, app_schema, batch_number, batch_size):

    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    subscriptionID = credentials.get("subscriptionID")

    prev_run_delta_query = f"""
    SELECT NVL(MAX(delta_end_date), '1990-01-01') AS previous_run_delta_end_date 
    FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL 
    WHERE load_status = 1
    """
    previous_run_delta_end_date = session.sql(prev_run_delta_query).collect()[0]["PREVIOUS_RUN_DELTA_END_DATE"]

    auth_proc = "CALL PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
    auth_token = session.sql(auth_proc).collect()["SALESFORCE_AUTH_TOKEN_GEN"]

    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionID,
        'Authorization': f'Bearer {auth_token}'
    }

    offset = (batch_number - 1) * batch_size
    contact_listing_query = f"""
    SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID, CONTACTID
    FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
    WHERE ROW_CRE_DT > '{previous_run_delta_end_date}'
    ORDER BY ROW_CRE_DT
    LIMIT {batch_size} OFFSET {offset}
    """
    contact_listing = session.sql(contact_listing_query).collect()

    url_query = f"""select url as URL from PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS WHERE TARGET_TABLE_NAME='SALESFORCE_CONTACT_DATA_UPDATE'"""
    url_link = session.sql(url_query).collect()
    salesforce_contact_update_url = [row["URL"] for row in url_link][0]

    for sf_contact in contact_listing:
        salesforce_contact_data_update_id = sf_contact

        upload_data_query = f"""
        SELECT OBJECT_CONSTRUCT('SalesforceAccountId', SalesforceAccountId, 'contactsInfo', 
        ARRAY_AGG(OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_CONTACT_DATA_UPDATE_ID, SalesforceAccountId, ROW_CRE_DT, ROW_CRE_USR_ID)))) AS JSON_OUTPUT
        FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE 
        WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID='{salesforce_contact_data_update_id}'
        GROUP BY SalesforceAccountId
        """
        contact_data_update = session.sql(upload_data_query).collect()[0]["JSON_OUTPUT"]

        response = requests.put(salesforce_contact_update_url, headers=headers, json=contact_data_update)

        audit_log_query = f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
        SELECT '{salesforce_contact_data_update_id}' AS SALESFORCE_CONTACT_DATA_UPDATE_ID, 
        '{contact_data_update}' AS REQUEST_JSON, 
        '{response.status_code}' AS RESPONSE_STATUS, 
        '{response.text}' AS RESPONSE_JSON, 
        CURRENT_TIMESTAMP AS ROW_CRE_DT, 'SFADMIN' AS ROW_CRE_USR_ID
        """
        session.sql(audit_log_query).collect()

    return f"Batch {batch_number} completed successfully"

def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP: str, BATCH_NUMBER: int, BATCH_SIZE: int):
    app_db_schema_query = f"""
    select SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA 
    from PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL 
    WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT' AND SALESFORCE_UPDATE_FROM_APP='{SALESFORCE_UPDATE_FROM_APP}'
    """
    app_db_return = session.sql(app_db_schema_query).collect()
    app_db = [row["SALESFORCE_UPDATE_FROM_APP_DB"] for row in app_db_return][0]
    app_schema = [row["SALESFORCE_UPDATE_FROM_APP_SCHEMA"] for row in app_db_return]

    return salesforce_contact_update_process_batch(session, app_db, app_schema, BATCH_NUMBER, BATCH_SIZE)
$$;
