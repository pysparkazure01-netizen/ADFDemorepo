-- Take the backup of the current procedure before compiling this one: PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.MARKETING_SALESFORCE_CONTACT_UPDATE()

USE ROLE ENTERPRISE_ADMIN_INTERNAL_ONSHORE;

CREATE OR REPLACE PROCEDURE PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.MARKETING_SALESFORCE_CONTACT_UPDATE()
RETURNS string
LANGUAGE PYTHON
RUNTIME_VERSION = 3.9
HANDLER = 'main'
PACKAGES = ('pyjwt', 'cryptography', 'requests', 'simplejson', 'snowflake-snowpark-python', 'pandas')
EXECUTE AS OWNER
AS
$$
import _snowflake
import simplejson as json
import requests
import snowflake.snowpark as snowpark
import pandas as pd
from snowflake.snowpark.functions import col

def main(session: snowpark.Session):
    
    # Run Salesforce Contact Update Framework
    contact_update_proc = "CALL PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE('MARKETING_CONTACT_MATRIX_CONTACT_UPDATE')"
    contact_update_return = session.sql(contact_update_proc).collect()[0]["SALESFORCE_CONTACT_UPDATE"]
    
    return contact_update_return

$$