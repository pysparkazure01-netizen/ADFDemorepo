---Step6--Create Framework Proc

CREATE OR REPLACE PROCEDURE UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_UPDATE(SALESFORCE_UPDATE_FROM_APP VARCHAR)
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
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
import pandas as pd
from snowflake.snowpark.functions import col

def salesforce_account_update_process(session: snowpark.Session,app_db,app_schema,load_nbr):
    # Get credentials from secrets
    credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    subscriptionID = credentials.get("subscriptionID")

    # Get previous run delta date
    prev_run_delta_query = f"""SELECT NVL(MAX(delta_end_date), '1990-01-01') AS previous_run_delta_end_date FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL WHERE load_status = 1"""
    previous_run_delta_end_date = session.sql(prev_run_delta_query).collect()[0]["PREVIOUS_RUN_DELTA_END_DATE"]

    # Fetch authentication token
    auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
    auth_token = session.sql(auth_proc).collect()[0]["SALESFORCE_AUTH_TOKEN_GEN"]

    # Add entry into control table
    cntrl_table_query = f"""
    INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL 
    SELECT {load_nbr}, '{previous_run_delta_end_date}', NULL, CURRENT_TIMESTAMP, NULL, 0, CURRENT_TIMESTAMP, 'SFADMIN'
    """
    session.sql(cntrl_table_query).collect()

    # Prepare headers
    headers = {
        'Ocp-Apim-Subscription-Key': subscriptionID,
        'Authorization': f'Bearer {auth_token}'
    }

    # Fetch account listing data
    account_listing_data_query = f"""
    SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID, SALESFORCEACCOUNTID
	FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
	WHERE row_cre_dt > '{previous_run_delta_end_date}'
    """
    account_listing = session.sql(account_listing_data_query).select(col('"SALESFORCE_ACCOUNT_DATA_UPDATE_ID"'),col('"SALESFORCEACCOUNTID"')).collect()

    # Salesforce update URL
    target_table_name = 'SALESFORCE_ACCOUNT_DATA_UPDATE'
    url_query = f"""select url as URL from UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS WHERE TARGET_TABLE_NAME='{target_table_name}'
    """
    url_link = session.sql(url_query).collect()
    salesforce_account_update_url = [row["URL"] for row in url_link][0] 

    # Initialize list for capturing responses
    response_text = []

    # Process each record
    for sf_account_listing in account_listing:
        # Fetch account update data
        SALESFORCE_ACCOUNT_DATA_UPDATE_ID = sf_account_listing[0]
        salesforce_account_id = sf_account_listing[1]
        upload_data_query = f"""
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(* EXCLUDE (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID))) AS JSON_OUTPUT 
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID='{SALESFORCE_ACCOUNT_DATA_UPDATE_ID}'
        """
        account_data_update = session.sql(upload_data_query).collect()[0]["JSON_OUTPUT"]
        # Process each record
        for record in json.loads(account_data_update):
            response = requests.put(salesforce_account_update_url, headers=headers, json=record)
            response_text=response.text
            sf_account_query = f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
            SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID, '{account_data_update}' as REQUEST_JSON, '{response}' as RESPONSE_STATUS, '{response_text}' as RESPONSE_JSON, CURRENT_TIMESTAMP AS ROW_CRE_DT, ROW_CRE_USR_ID 
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID='{SALESFORCE_ACCOUNT_DATA_UPDATE_ID}'
            """
            data_return = session.sql(sf_account_query).collect()

def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):
    
    app_db_schema_query = f"""select SALESFORCE_UPDATE_FROM_APP_DB, SALESFORCE_UPDATE_FROM_APP_SCHEMA from UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL WHERE SALESFORCE_UPDATE_OBJECT = 'ACCOUNT' AND SALESFORCE_UPDATE_FROM_APP='{SALESFORCE_UPDATE_FROM_APP}'
    """
    app_db_return = session.sql(app_db_schema_query).collect()
    app_db = [row["SALESFORCE_UPDATE_FROM_APP_DB"] for row in app_db_return][0]
    app_schema = [row["SALESFORCE_UPDATE_FROM_APP_SCHEMA"] for row in app_db_return][0]
    
    # Get load status
    load_status_query = f"""SELECT nvl(min(LOAD_STATUS),1) AS LOAD_STATUS FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL"""
    load_status = session.sql(load_status_query).collect()[0]["LOAD_STATUS"]
	
    if load_status == 0 :
    
        return f"Previous load is in progress"
		
    else:

        # Generate load number
        load_nbr_query = f"""SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.nextval AS LOAD_NBR"""
        load_nbr = session.sql(load_nbr_query).collect()[0]["LOAD_NBR"]
    
		# Get delta end date
        delta_end_date_query = f"""SELECT MAX(ROW_CRE_DT) AS delta_end_date FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE"""
        delta_end_date = session.sql(delta_end_date_query).collect()[0]["DELTA_END_DATE"]
		
        # Salesforce Account Upd API and get response
        json_data = salesforce_account_update_process(session,app_db,app_schema,load_nbr)
	
		# Update Audit Entry When Proc Complete Success
        cntrl_table_update_query = f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS=1, DELTA_END_DATE = '{delta_end_date}', LOAD_END_DATE=GETDATE()
        WHERE LOAD_NBR={load_nbr}
        """
        session.sql(cntrl_table_update_query).collect()
	
        return f"Salesforce Account Update Batch Completed Successfully"	

$$;