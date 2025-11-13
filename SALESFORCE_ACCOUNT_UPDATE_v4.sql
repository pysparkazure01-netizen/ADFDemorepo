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

# ============================================================================================
# HELPER: Send a single record to API (PUT) – Swallow all exceptions, return audit info only
# ============================================================================================
def send_record(session, app_db, app_schema, rec_id, base_url, headers):
    try:
        # Fetch payload for this Salesforce Account
        payload_query = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (
                SALESFORCE_ACCOUNT_DATA_UPDATE_ID,
                ROW_CRE_DT, ROW_CRE_USR_ID
            )) AS REC
            FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
            WHERE SALESFORCE_ACCOUNT_DATA_UPDATE_ID = '{rec_id}'
        """
        row = session.sql(payload_query).collect()
        if not row:
            payload = {}
        else:
            r = row[0]["REC"]
            try:
                payload = dict(r)
            except:
                payload = json.loads(str(r))

        # Call API
        try:
            resp = requests.put(base_url, headers=headers, json=payload, timeout=60)
            status = getattr(resp, "status_code", None)
            try:
                rjson = resp.json()
            except:
                rjson = {"raw": resp.text}
        except Exception as e:
            status = None
            rjson = {"exception": str(e)}

        return {
            "id": rec_id,
            "request": payload,
            "status": status,
            "response": rjson
        }

    except Exception as e:
        # Global fallback
        return {
            "id": rec_id,
            "request": {},
            "status": None,
            "response": {"exception": str(e)}
        }


# ============================================================================================
# MAIN PROCEDURE HANDLER
# ============================================================================================
def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    MAX_WORKERS = 15  # Parallel calls per batch

    # ----------------------------------------------------------------------------------------
    # 1. Resolve DB & Schema from control table
    # ----------------------------------------------------------------------------------------
    cfg_query = f"""
        SELECT SALESFORCE_UPDATE_FROM_APP_DB AS DBN,
               SALESFORCE_UPDATE_FROM_APP_SCHEMA AS SCH
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
        WHERE SALESFORCE_UPDATE_OBJECT = 'ACCOUNT'
          AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
        LIMIT 1
    """
    cfg = session.sql(cfg_query).collect()
    if not cfg:
        raise Exception(f"Config not found for {SALESFORCE_UPDATE_FROM_APP}")

    app_db = cfg[0]["DBN"]
    app_schema = cfg[0]["SCH"]


    # ----------------------------------------------------------------------------------------
    # 2. Generate LOAD_NBR
    # ----------------------------------------------------------------------------------------
    load_nbr = session.sql(
        f"SELECT {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS N"
    ).collect()[0]["N"]


    # ----------------------------------------------------------------------------------------
    # 3. Previous successful delta end
    # ----------------------------------------------------------------------------------------
    prev_query = f"""
        SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        WHERE LOAD_STATUS = 1
    """
    previous_delta = session.sql(prev_query).collect()[0]["PREV"]


    # ----------------------------------------------------------------------------------------
    # 4. Compute new delta_end (same logic as Contact proc)
    # ----------------------------------------------------------------------------------------
    delta_query = f"""
        SELECT MAX(ROW_CRE_DT) AS DELTA_END
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
    """
    delta_row = session.sql(delta_query).collect()[0]
    delta_end = delta_row["DELTA_END"]

    if delta_end is None:
        delta_end = session.sql("SELECT CURRENT_TIMESTAMP AS TS").collect()[0]["TS"]


    # ----------------------------------------------------------------------------------------
    # 5. Insert audit control START record
    # ----------------------------------------------------------------------------------------
    session.sql(f"""
        INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE,
         LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
        VALUES ({load_nbr}, '{previous_delta}', NULL, CURRENT_TIMESTAMP(), NULL,
                0, CURRENT_TIMESTAMP(), 'SFADMIN')
    """).collect()


    # ----------------------------------------------------------------------------------------
    # 6. Get API base URL
    # ----------------------------------------------------------------------------------------
    ep = session.sql(f"""
        SELECT URL
        FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
        WHERE TARGET_TABLE_NAME = 'SALESFORCE_ACCOUNT_DATA_UPDATE'
        LIMIT 1
    """).collect()[0]

    base_url = ep["URL"]


    # ----------------------------------------------------------------------------------------
    # 7. Load Secret & Get Bearer Token (same as Contact)
    # ----------------------------------------------------------------------------------------
    try:
        cred_raw = _snowflake.get_generic_secret_string('cred')
        cred = json.loads(cred_raw)
    except:
        cred = {}

    subscriptionID = cred.get("subscriptionID")

    # Auth token
    try:
        auth_proc = "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
        auth_res = session.sql(auth_proc).collect()
        token = auth_res[0][0] if auth_res else None
    except:
        token = None

    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if token:
        headers["Authorization"] = f"Bearer {token}"


    # ----------------------------------------------------------------------------------------
    # 8. Get all records to process between (previous_delta → delta_end)
    # ----------------------------------------------------------------------------------------
    list_query = f"""
        SELECT SALESFORCE_ACCOUNT_DATA_UPDATE_ID
        FROM {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE
        WHERE ROW_CRE_DT > '{previous_delta}'
          AND ROW_CRE_DT <= '{delta_end}'
        ORDER BY ROW_CRE_DT
    """
    listing = session.sql(list_query).collect()
    total = len(listing)

    if total == 0:
        session.sql(f"""
            UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
            SET LOAD_STATUS = 1,
                DELTA_END_DATE = '{delta_end}',
                LOAD_END_DATE = CURRENT_TIMESTAMP()
            WHERE LOAD_NBR = {load_nbr}
        """).collect()
        return f"No records to process. LOAD_NBR={load_nbr}"


    # ----------------------------------------------------------------------------------------
    # 9. Process in batches using ThreadPoolExecutor
    # ----------------------------------------------------------------------------------------
    audit_logs = []
    batch_size = MAX_WORKERS

    for batch_no, start in enumerate(range(0, total, batch_size), start=1):
        batch = listing[start:start + batch_size]
        print(f"Batch {batch_no} start: {len(batch)} records")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [
                pool.submit(send_record, session, app_db, app_schema,
                            r["SALESFORCE_ACCOUNT_DATA_UPDATE_ID"], base_url, headers)
                for r in batch
            ]

            for f in as_completed(futures):
                try:
                    audit_logs.append(f.result())
                except Exception as e:
                    audit_logs.append({
                        "id": None,
                        "request": {},
                        "status": None,
                        "response": {"exception": str(e)}
                    })

        print(f"Batch {batch_no} complete")


    # ----------------------------------------------------------------------------------------
    # 10. Insert Audit Log in bulk
    # ----------------------------------------------------------------------------------------
    if audit_logs:
        values = []
        for a in audit_logs:
            rid = a["id"]
            req = json.dumps(a["request"]).replace("'", "''")
            resp = json.dumps(a["response"]).replace("'", "''")
            status = a["status"]

            values.append(
                f"('{rid}','{req}','{status}','{resp}',CURRENT_TIMESTAMP(),'SFADMIN')"
            )

        session.sql(f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_LOG
            (SALESFORCE_ACCOUNT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON,
             ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES {",".join(values)}
        """).collect()


    # ----------------------------------------------------------------------------------------
    # 11. Mark audit control as SUCCESS ALWAYS (per your requirement)
    # ----------------------------------------------------------------------------------------
    session.sql(f"""
        UPDATE {app_db}.{app_schema}.SALESFORCE_ACCOUNT_DATA_UPDATE_AUDIT_CONTROL
        SET LOAD_STATUS = 1,
            DELTA_END_DATE = '{delta_end}',
            LOAD_END_DATE = CURRENT_TIMESTAMP()
        WHERE LOAD_NBR = {load_nbr}
    """).collect()


    return f"ACCOUNT update completed: LOAD_NBR={load_nbr}, Records={total}"

$$;
