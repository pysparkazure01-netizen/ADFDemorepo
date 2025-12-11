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
import json
import time
import requests
import snowflake.snowpark as snowpark
import _snowflake
from concurrent.futures import ThreadPoolExecutor, as_completed


MAX_WORKERS = 15
BIG_BATCH_SIZE = 2000
COOLDOWN_SECONDS = 120
MAX_RETRIES = 3
RETRY_DELAYS = [1, 3, 6]

def refresh_auth_token(session):
    try:
        res = session.sql(
            "CALL UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_AUTH_TOKEN_GEN('SALESFORCE_AUTH_TOKEN')"
        ).collect()
        if res:
            return list(res[0].values())[0]
    except Exception:
        return None
    return None

def build_headers(token):
    try:
        credentials = json.loads(_snowflake.get_generic_secret_string('cred'), strict=False)
    except Exception:
        credentials = {}
    subscriptionID = credentials.get("subscriptionID")

    headers = {"Content-Type": "application/json"}
    if subscriptionID:
        headers["Ocp-Apim-Subscription-Key"] = subscriptionID
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def send_one(session, app_db, app_schema, rec_id, base_url, token_getter):
    try:
        payload_q = f"""
            SELECT OBJECT_CONSTRUCT(* EXCLUDE (
                SALESFORCE_CONTACT_DATA_UPDATE_ID, ROW_CRE_DT, ROW_CRE_USR_ID
            )) AS RECORD_DATA
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE SALESFORCE_CONTACT_DATA_UPDATE_ID = '{rec_id}'
        """
        rows = session.sql(payload_q).collect()
        if not rows:
            req_obj = {}
        else:
            rec_obj = rows[0]["RECORD_DATA"]
            try:
                req_obj = dict(rec_obj) if isinstance(rec_obj, dict) else json.loads(str(rec_obj))
            except Exception:
                try:
                    req_obj = json.loads(str(rec_obj).replace("'", '"'))
                except Exception:
                    req_obj = {}

        last_status = None
        last_resp = None

        for attempt in range(MAX_RETRIES):
            try:
                token = token_getter()
                headers = build_headers(token)
                resp = requests.put(base_url, headers=headers, json=req_obj, timeout=60)

                status = getattr(resp, "status_code", None)
                last_status = status

                try:
                    last_resp = resp.json()
                except Exception:
                    last_resp = {"raw": resp.text if hasattr(resp, "text") else str(resp)}

                if status == 401:
                    time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                    continue

                if status in (429, 500, 502, 503, 504):
                    time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                    continue

                break

            except Exception as e_req:
                last_status = None
                last_resp = {"exception": str(e_req)}
                time.sleep(RETRY_DELAYS[min(attempt, len(RETRY_DELAYS)-1)])
                continue

        return {
            "id": rec_id,
            "request": req_obj,
            "status": last_status,
            "response": last_resp
        }

    except Exception as e_outer:
        return {"id": rec_id, "request": {}, "status": None, "response": {"exception": str(e_outer)}}


def main(session: snowpark.Session, SALESFORCE_UPDATE_FROM_APP):

    load_nbr = None
    delta_end = None
    app_db = None
    app_schema = None

    try:

        cfg_q = f"""
            SELECT SALESFORCE_UPDATE_FROM_APP_DB AS DBNAME,
                   SALESFORCE_UPDATE_FROM_APP_SCHEMA AS SCHEMA_NAME
            FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_UPDATE_PROCESS_CNTRL
            WHERE SALESFORCE_UPDATE_OBJECT = 'CONTACT'
              AND SALESFORCE_UPDATE_FROM_APP = '{SALESFORCE_UPDATE_FROM_APP}'
            LIMIT 1
        """
        cfg = session.sql(cfg_q).collect()
        if not cfg:
            raise Exception("Missing config row for CONTACT")

        app_db = cfg[0]["DBNAME"]
        app_schema = cfg[0]["SCHEMA_NAME"]

        load_nbr = session.sql(
            f"SELECT {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_LOAD_NBR.NEXTVAL AS NBR"
        ).collect()[0]["NBR"]

        prev_delta_q = f"""
            SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') AS PREV
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            WHERE LOAD_STATUS = 1
        """
        prev_delta = session.sql(prev_delta_q).collect()[0]["PREV"]

        delta_end_q = f"""
            SELECT MAX(ROW_CRE_DT) AS END_TS
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
        """
        delta_end_res = session.sql(delta_end_q).collect()[0]["END_TS"]
        delta_end = delta_end_res if delta_end_res else session.sql("SELECT CURRENT_TIMESTAMP() AS T").collect()[0]["T"]

        session.sql(f"""
            INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
            (LOAD_NBR, DELTA_START_DT, DELTA_END_DATE, LOAD_START_DATE, LOAD_END_DATE,
             LOAD_STATUS, ROW_CRE_DT, ROW_CRE_USR_ID)
            VALUES ({load_nbr}, '{prev_delta}', NULL, CURRENT_TIMESTAMP(), NULL,
                    0, CURRENT_TIMESTAMP(), 'SFADMIN')
        """).collect()

        url_row = session.sql(f"""
            SELECT URL
            FROM UAT_ADS.DWNSTRM_SALESFORCE.SALESFORCE_PARAM_API_ENDPOINTS
            WHERE TARGET_TABLE_NAME = 'SALESFORCE_CONTACT_DATA_UPDATE'
            LIMIT 1
        """).collect()
        base_url = url_row[0]["URL"]

        token = refresh_auth_token(session)
        token_holder = [token]

        def token_getter():
            return token_holder[0]

        def refresh_and_update():
            new_token = refresh_auth_token(session)
            if new_token:
                token_holder[0] = new_token
            return token_holder[0]

        listing_q = f"""
            SELECT SALESFORCE_CONTACT_DATA_UPDATE_ID AS ID
            FROM {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE
            WHERE ROW_CRE_DT > '{prev_delta}'
              AND ROW_CRE_DT <= '{delta_end}'
            ORDER BY ROW_CRE_DT
        """
        rows = session.sql(listing_q).collect()
        ids = [r["ID"] for r in rows]
        total = len(ids)

        if total == 0:
            return "No records to process"

        idx = 0
        for batch_num, start in enumerate(range(0, total, BIG_BATCH_SIZE), start=1):
            big_batch = ids[start:start + BIG_BATCH_SIZE]
            print(f"Starting BIG BATCH {batch_num} — {len(big_batch)} records")

            audit_entries = []

            for s in range(0, len(big_batch), MAX_WORKERS):
                chunk = big_batch[s:s+MAX_WORKERS]

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                    futures = {pool.submit(send_one, session, app_db, app_schema, rec_id, base_url, token_getter): rec_id
                               for rec_id in chunk}

                    for fut in as_completed(futures):
                        try:
                            audit_entries.append(fut.result())
                        except Exception as e:
                            audit_entries.append({
                                "id": futures[fut],
                                "request": {},
                                "status": None,
                                "response": {"exception": str(e)}
                            })

            if audit_entries:
                values = []
                for a in audit_entries:
                    rid = a["id"]
                    req = json.dumps(a["request"]).replace("'", "''")
                    resp = json.dumps(a["response"]).replace("'", "''")
                    status = a["status"]
                    values.append(
                        f"('{rid}','{req}','{status}','{resp}',CURRENT_TIMESTAMP(),'SFADMIN')"
                    )

                session.sql(f"""
                    INSERT INTO {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_LOG
                    (SALESFORCE_CONTACT_DATA_UPDATE_ID, REQUEST_JSON, RESPONSE_STATUS, RESPONSE_JSON,
                     ROW_CRE_DT, ROW_CRE_USR_ID)
                    VALUES {','.join(values)}
                """).collect()

            refresh_and_update()

            try:
                session.sql(f"CALL SYSTEM$WAIT({COOLDOWN_SECONDS})").collect()
            except Exception:
                time.sleep(COOLDOWN_SECONDS)

    except Exception as e:
        print("Top-level exception:", e)

    finally:
        if load_nbr and app_db and app_schema and delta_end:
            session.sql(f"""
                UPDATE {app_db}.{app_schema}.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL
                SET LOAD_STATUS = 1,
                    DELTA_END_DATE = '{delta_end}',
                    LOAD_END_DATE = CURRENT_TIMESTAMP()
                WHERE LOAD_NBR = {load_nbr}
            """).collect()

    return f"Completed CONTACT update | LOAD_NBR={load_nbr} | Records={len(ids)}"
$$;
