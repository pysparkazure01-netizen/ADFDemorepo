CREATE OR REPLACE PROCEDURE PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE_ASYNC(SALESFORCE_UPDATE_FROM_APP VARCHAR)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    v_total_records INT;
    v_batch_size INT DEFAULT 50;  -- Tune batch size as needed
    v_num_batches INT;
    v_batch_num INT;
BEGIN
    SELECT COUNT(*) INTO v_total_records
    FROM PROD_ADS.<your_schema>.SALESFORCE_CONTACT_DATA_UPDATE
    WHERE ROW_CRE_DT > (SELECT NVL(MAX(DELTA_END_DATE), '1990-01-01') FROM PROD_ADS.<your_schema>.SALESFORCE_CONTACT_DATA_UPDATE_AUDIT_CONTROL WHERE LOAD_STATUS = 1);

    SET v_num_batches = CEIL(v_total_records / v_batch_size);

    FOR v_batch_num IN 1..v_num_batches DO
        ASYNC (
            CALL PROD_ADS.DWNSTRM_SALESFORCE.SALESFORCE_CONTACT_UPDATE_BATCH(
                SALESFORCE_UPDATE_FROM_APP => SALESFORCE_UPDATE_FROM_APP,
                BATCH_NUMBER => v_batch_num,
                BATCH_SIZE => v_batch_size
            )
        );
    END FOR;

    AWAIT ALL;

    -- Optionally update audit control table here to mark overall load complete

    RETURN 'All batches completed successfully';
END;
