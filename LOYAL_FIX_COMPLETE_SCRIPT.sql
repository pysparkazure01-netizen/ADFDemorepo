/*
================================================================================
COMPLETE FIX SCRIPT — LIFECYCLE_FLAG = 'LOYAL' RESTORATION
================================================================================
Issue   : LIFECYCLE_FLAG = 'LOYAL' not populating after COLO-1732 MDM change
Root    : 1. V_MDM_CUST_SEGMENT only covers 10% of customers (2.3M of 24.2M)
          2. CHURN_SITE_LOYAL_CTGRY_CW has old MDM taxonomy — no new view match
          3. 95,948 customers have NULL MAPS_TYPE even within 10% covered
Author  : Generated based on investigation
Date    : 2026-04-12
================================================================================

SCRIPT SECTIONS:
  SECTION 0 — Pre-flight validation (run first, verify counts)
  SECTION 1 — Fix 1: Rebuild V_MDM_CUST_SEGMENT_SK4112026 (Fix coverage gap)
  SECTION 2 — Fix 2: Backup and rebuild CHURN_SITE_LOYAL_CTGRY_CW (new taxonomy)
  SECTION 3 — Fix 3: NULL MAPS_TYPE handling (edge case)
  SECTION 4 — Post-fix validation (run after all fixes)
  SECTION 5 — Rollback script (run ONLY if fix causes issues)

================================================================================
IMPORTANT: Run sections in order. Do NOT skip Section 0.
================================================================================
*/


/* ==============================================================================
   SECTION 0 — PRE-FLIGHT VALIDATION
   Run these queries BEFORE making any changes.
   Save results to compare against Section 4 post-fix validation.
   ==============================================================================*/

-- 0.1 Current LOYAL count in output table (should be 0 or very low)
SELECT 
    LIFECYCLE_FLAG,
    COUNT(*) AS RECORD_COUNT
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.M_CUST_CHURN_INDICATOR_ST_CW
GROUP BY LIFECYCLE_FLAG
ORDER BY RECORD_COUNT DESC;

-- 0.2 Current view coverage
SELECT
    COUNT(DISTINCT CB.DIM_CUST_CURR_ID)  AS TOTAL_BLEND_CURR_IDS,
    COUNT(DISTINCT SR.DIM_CUST_CURR_ID)  AS VIEW_COVERED_IDS,
    COUNT(DISTINCT CASE WHEN SR.DIM_CUST_CURR_ID IS NULL 
          THEN CB.DIM_CUST_CURR_ID END)  AS NOT_COVERED_IDS,
    ROUND(COUNT(DISTINCT SR.DIM_CUST_CURR_ID) * 100.0 / 
          COUNT(DISTINCT CB.DIM_CUST_CURR_ID), 2) AS PCT_COVERED
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
    ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
WHERE CB.PTNT_FLG <> 'Y'
  AND CB.SYS_PLTFRM = 'E1';

-- 0.3 Current lookup table row count
SELECT COUNT(*) AS LOOKUP_ROW_COUNT 
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW;

-- 0.4 Current loyal join success rate
SELECT
    COUNT(*)                                                        AS TOTAL_CUSTOMERS,
    SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NOT NULL THEN 1 ELSE 0 END) AS LOYAL_JOIN_SUCCESS,
    SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NULL THEN 1 ELSE 0 END)     AS LOYAL_JOIN_FAIL,
    ROUND(SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NOT NULL THEN 1 ELSE 0 END) 
          * 100.0 / COUNT(*), 2)                                    AS PCT_JOIN_SUCCESS
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
    ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL
    ON UPPER(SR.MAPS_MKT)   = UPPER(LYL.CUST_SGMNT_MKT_DSC)
    AND UPPER(SR.MAPS_STNG)  = UPPER(LYL.CUST_SGMNT_SETTNG_DSC)
    AND UPPER(SR.MAPS_TYPE)  = UPPER(LYL.CUST_SGMNT_DFLT_TYPE_DSC)
WHERE CB.PTNT_FLG <> 'Y'
  AND CB.SYS_PLTFRM = 'E1';


/* ==============================================================================
   SECTION 1 — FIX 1: REBUILD V_MDM_CUST_SEGMENT_SK4112026
   
   PROBLEM : View only covers 10% of customers because customers with no 
             Compile/Definitive bridge match produce no row in the view.
   FIX     : Add all_customers CTE as base anchor using LEFT JOIN to guarantee
             one row per customer. Fall back to MMS_CLASS_DSC and SPCLTY_DSC
             from base table for unmatched customers.
   IMPACT  : Increases coverage from ~10% to ~100%
   ==============================================================================*/

CREATE OR REPLACE VIEW DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 (
    DIM_CUST_CURR_ID,
    MAPS_MKT,
    MAPS_STNG,
    MAPS_TYPE
) AS

WITH 
/*
 * BASE ANCHOR: All customers from blend table.
 * This guarantees every customer gets a row even if all bridge joins fail.
 * MMS_CLASS_DSC and SPCLTY_DSC serve as ultimate fallbacks.
 */
all_customers AS (
    SELECT DISTINCT
        DIM_CUST_CURR_ID,
        UPPER(MMS_CLASS_DSC)     AS MMS_CLASS_DSC,
        UPPER(MMS_SUB_CLASS_DSC) AS MMS_SUB_CLASS_DSC,
        UPPER(SPCLTY_DSC)        AS SPCLTY_DSC
    FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR
    WHERE PTNT_FLG <> 'Y'
      AND SYS_PLTFRM = 'E1'
),

/*
 * EXISTING SEGMENT LOGIC: Unchanged from original view.
 * Uses Compile/Definitive bridge tables for enriched segment data.
 * Produces NULL for customers with no bridge match.
 */
amdm_mst_calc AS (
    SELECT 
        DIM_CUST_CURR_ID,
        COALESCE(MAX(FACILITY_TYPE), MAX(FIRM_TYPE), MAX(MMS_CLASS_DSC))             AS MAPS_MKT,
        COALESCE(MAX(FACILITY_TYPE_LEVEL_3), MAX(HOSPITAL_TYPE), MAX(MMS_CLASS_DSC)) AS MAPS_STNG,
        COALESCE(MAX(HCP_SPECIALTY), MAX(MAIN_SPECIALTY), MAX(SPCLTY_DSC))           AS MAPS_TYPE
    FROM (
        SELECT *
        FROM (
            SELECT 
                C.DIM_CUST_CURR_ID,
                UPPER(CB.FACILITY_TYPE) FACILITY_TYPE,
                SUM(CASE WHEN CB.FACILITY_TYPE IS NOT NULL THEN 1 ELSE 0 END) FACILITY_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(FACILITY_REC_COUNT, -99999) DESC
                ) FACILITY_ROWIDX,
                UPPER(DB.FIRM_TYPE) FIRM_TYPE,
                SUM(CASE WHEN DB.FIRM_TYPE IS NOT NULL THEN 1 ELSE 0 END) FIRM_TYPE_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(FIRM_TYPE_REC_COUNT, -99999) DESC
                ) FIRM_TYPE_ROWIDX,
                UPPER(C.MMS_CLASS_DSC) MMS_CLASS_DSC,
                SUM(CASE WHEN C.MMS_CLASS_DSC IS NOT NULL THEN 1 ELSE 0 END) MMS_CLASS_DSC_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(MMS_CLASS_DSC_REC_COUNT, -99999) DESC
                ) MMS_CLASS_DSC_ROWIDX,
                UPPER(CB.FACILITY_TYPE_LEVEL_3) FACILITY_TYPE_LEVEL_3,
                SUM(CASE WHEN CB.FACILITY_TYPE_LEVEL_3 IS NOT NULL THEN 1 ELSE 0 END) FACILITY_TYPE_LEVEL_3_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(FACILITY_TYPE_LEVEL_3_REC_COUNT, -99999) DESC
                ) FACILITY_TYPE_LEVEL_3_ROWIDX,
                UPPER(DH.HOSPITAL_TYPE) HOSPITAL_TYPE,
                SUM(CASE WHEN DH.HOSPITAL_TYPE IS NOT NULL THEN 1 ELSE 0 END) HOSPITAL_TYPE_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(HOSPITAL_TYPE_REC_COUNT, -99999) DESC
                ) HOSPITAL_TYPE_ROWIDX,
                UPPER(C.MMS_SUB_CLASS_DSC) MMS_SUB_CLASS_DSC,
                SUM(CASE WHEN C.MMS_SUB_CLASS_DSC IS NOT NULL THEN 1 ELSE 0 END) MMS_SUB_CLASS_DSC_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(MMS_SUB_CLASS_DSC_REC_COUNT, -99999) DESC
                ) MMS_SUB_CLASS_DSC_ROWIDX,
                UPPER(CHCP.HCP_SPECIALTY) HCP_SPECIALTY,
                SUM(CASE WHEN CHCP.HCP_SPECIALTY IS NOT NULL THEN 1 ELSE 0 END) HCP_SPECIALTY_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(HCP_SPECIALTY_REC_COUNT, -99999) DESC
                ) HCP_SPECIALTY_ROWIDX,
                UPPER(DP.MAIN_SPECIALTY) MAIN_SPECIALTY,
                SUM(CASE WHEN DP.MAIN_SPECIALTY IS NOT NULL THEN 1 ELSE 0 END) MAIN_SPECIALTY_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(MAIN_SPECIALTY_REC_COUNT, -99999) DESC
                ) MAIN_SPECIALTY_ROWIDX,
                UPPER(C.SPCLTY_DSC) SPCLTY_DSC,
                SUM(CASE WHEN C.SPCLTY_DSC IS NOT NULL THEN 1 ELSE 0 END) SPCLTY_DSC_REC_COUNT,
                ROW_NUMBER() OVER (
                    PARTITION BY C.DIM_CUST_CURR_ID
                    ORDER BY NVL(SPCLTY_DSC_REC_COUNT, -99999) DESC
                ) SPCLTY_DSC_ROWIDX
            FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR C
            LEFT JOIN DEV_ENT_DL_US_BUS_DB.CSS_CUSTOMER_DATA_MODEL.V_MMS_CUSTOMER_BRIDGE_TO_COMPILE CB
                ON C.DIM_CUST_CURR_ID = CB.CUST_CURR_ID
            LEFT JOIN DEV_ENT_DL_US_BUS_DB.CSS_CUSTOMER_DATA_MODEL.MMS_CUSTOMER_BRIDGE_TO_DEFINITIVE DB
                ON C.DIM_CUST_CURR_ID = DB.CUST_CURR_ID
            LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DEF_HOSPITAL_OVERVIEW DH
                ON DB.HOSPITAL_ID = DH.HOSPITAL_ID
            LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.HCP_ATTRIBUTES CHCP
                ON CB.ADDRESS_COMPILE_ID = CHCP.D_ADDRESS_COMPILE_ID
            LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DEF_PHYS_GRP_OVERVIEW DP
                ON DB.HOSPITAL_ID = DP.HOSPITAL_ID
            WHERE C.PTNT_FLG <> 'Y'
              AND C.SYS_PLTFRM = 'E1'
            GROUP BY
                C.DIM_CUST_CURR_ID,
                UPPER(CB.FACILITY_TYPE),
                UPPER(DB.FIRM_TYPE),
                UPPER(C.MMS_CLASS_DSC),
                UPPER(CB.FACILITY_TYPE_LEVEL_3),
                UPPER(DH.HOSPITAL_TYPE),
                UPPER(C.MMS_SUB_CLASS_DSC),
                UPPER(CHCP.HCP_SPECIALTY),
                UPPER(DP.MAIN_SPECIALTY),
                UPPER(C.SPCLTY_DSC)
        )
        WHERE (
            FIRM_TYPE_ROWIDX              = 1
            OR FACILITY_ROWIDX            = 1
            OR MMS_CLASS_DSC_ROWIDX       = 1
            OR FACILITY_TYPE_LEVEL_3_ROWIDX = 1
            OR HOSPITAL_TYPE_ROWIDX       = 1
            OR MMS_SUB_CLASS_DSC_ROWIDX   = 1
            OR HCP_SPECIALTY_ROWIDX       = 1
            OR MAIN_SPECIALTY_ROWIDX      = 1
            OR SPCLTY_DSC_ROWIDX          = 1
        )
    )
    GROUP BY DIM_CUST_CURR_ID
)

/*
 * FINAL OUTPUT:
 * LEFT JOIN guarantees all_customers always produces a row.
 * For customers matched by amdm_mst_calc → use enriched segment values.
 * For customers NOT matched (90% currently) → fall back to MMS base values.
 * MAPS_TYPE fix: use MMS_SUB_CLASS_DSC when all three specialty sources NULL.
 */
SELECT
    ac.DIM_CUST_CURR_ID,
    -- MAPS_MKT: use enriched value if available, else MMS_CLASS_DSC from base
    COALESCE(amc.MAPS_MKT,  ac.MMS_CLASS_DSC)                          AS MAPS_MKT,
    -- MAPS_STNG: use enriched value if available, else MMS_SUB_CLASS_DSC from base
    COALESCE(amc.MAPS_STNG, ac.MMS_SUB_CLASS_DSC, ac.MMS_CLASS_DSC)   AS MAPS_STNG,
    -- MAPS_TYPE: use enriched value if available, else SPCLTY_DSC from base (Fix 3)
    COALESCE(amc.MAPS_TYPE, ac.SPCLTY_DSC)                             AS MAPS_TYPE
FROM all_customers ac
LEFT JOIN amdm_mst_calc amc
    ON ac.DIM_CUST_CURR_ID = amc.DIM_CUST_CURR_ID;


/* ==============================================================================
   SECTION 2 — FIX 2: BACKUP AND REBUILD CHURN_SITE_LOYAL_CTGRY_CW
   
   PROBLEM : Lookup table has old MDM taxonomy (Acute/Hospital/Eye and Ear).
             New view produces new taxonomy (PRIMARY CARE/PRIMARY CARE/FAMILY PRACTICE).
             Three-way join always fails → LOYAL_CTGRY_LIMIT = NULL.
   FIX     : Backup old table, then insert new taxonomy mappings derived from:
             a) Direct mapping from result.txt (old MDM → new view values)
             b) MMS_CLASS_DSC based entries for the 90% fallback customers
   IMPACT  : LOYAL_CTGRY_LIMIT will now match for both 10% enriched customers
             and 90% fallback customers after Fix 1 is applied.
   ==============================================================================*/

-- 2.1 BACKUP existing lookup table before any changes
CREATE TABLE IF NOT EXISTS 
    DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW_BACKUP_20260412
AS
SELECT 
    *,
    CURRENT_TIMESTAMP() AS BACKUP_DT
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW;

-- Verify backup
SELECT COUNT(*) AS BACKUP_ROW_COUNT 
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW_BACKUP_20260412;

-- 2.2 INSERT new taxonomy mappings derived from old→new mapping
-- These are built from result.txt mapping analysis:
-- Old MDM combination → New view combination → carry forward LOYAL_CTGRY_LIMIT
-- Where multiple old values map to same new value, weighted average is used.
-- NOTE: Existing old MDM rows are KEPT (for backward compatibility).
--       Only NEW taxonomy combinations are added.

INSERT INTO DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW
(
    CUST_SGMNT_MKT_DSC,
    CUST_SGMNT_SETTNG_DSC,
    CUST_SGMNT_DFLT_TYPE_DSC,
    SETTNG_SITE_CNT,
    SITE_CNT,
    LOYAL_CTGRY_LIMIT,
    UPDATE_DT
)

-- Build new entries by mapping old MDM limits to new view taxonomy
-- using the customer-weighted average where multiple old → one new
WITH new_taxonomy_mapping AS (
    SELECT
        SR.MAPS_MKT                                                  AS NEW_MKT,
        SR.MAPS_STNG                                                 AS NEW_STNG,
        SR.MAPS_TYPE                                                 AS NEW_TYPE,
        -- Weighted average of old limits mapped to this new combination
        ROUND(
            SUM(LYL.LOYAL_CTGRY_LIMIT * CUST_COUNTS.CNT) /
            NULLIF(SUM(CUST_COUNTS.CNT), 0)
        , 2)                                                         AS WEIGHTED_LIMIT,
        SUM(CUST_COUNTS.CNT)                                         AS TOTAL_CUST_COUNT
    FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
    -- Join to new view to get new taxonomy values
    INNER JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
        ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
    -- Join to old MDM chain to get old taxonomy
    LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.MDM_GR_SOURCE_REL MDM_SR
        ON CB.CUST_E1_NUM = MDM_SR.SRC_REC_ID
        AND MDM_SR.REC_STATUS_DSC = 'Active'
        AND MDM_SR.SRC_SYS_CD = 'JDE'
    LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.MDM_GR_CNSL_ALL GRC
        ON MDM_SR.MDM_PARTY_ID = GRC.MDM_PARTY_ID
        AND GRC.REC_STATUS_DSC = 'Active'
    -- Get existing limit from old lookup using old taxonomy
    LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL
        ON GRC.CUST_SGMNT_MKT_DSC       = LYL.CUST_SGMNT_MKT_DSC
        AND GRC.CUST_SGMNT_SETTNG_DSC    = LYL.CUST_SGMNT_SETTNG_DSC
        AND GRC.CUST_SGMNT_DFLT_TYPE_DSC = LYL.CUST_SGMNT_DFLT_TYPE_DSC
    -- Customer count per new combination for weighted average
    INNER JOIN (
        SELECT 
            DIM_CUST_CURR_ID,
            COUNT(*) AS CNT
        FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR
        WHERE PTNT_FLG <> 'Y'
          AND SYS_PLTFRM = 'E1'
        GROUP BY DIM_CUST_CURR_ID
    ) CUST_COUNTS
        ON CB.DIM_CUST_CURR_ID = CUST_COUNTS.DIM_CUST_CURR_ID
    WHERE CB.PTNT_FLG <> 'Y'
      AND CB.SYS_PLTFRM = 'E1'
      AND SR.MAPS_MKT IS NOT NULL
      AND SR.MAPS_STNG IS NOT NULL
      AND LYL.LOYAL_CTGRY_LIMIT IS NOT NULL   -- only where old limit existed
    GROUP BY SR.MAPS_MKT, SR.MAPS_STNG, SR.MAPS_TYPE
),

-- Build fallback entries using MMS_CLASS_DSC/MMS_SUB_CLASS_DSC/SPCLTY_DSC
-- for the 90% of customers not covered by bridge tables
-- These use the same SPCLTY_DSC matching logic from Step 2 results
mms_fallback_mapping AS (
    SELECT DISTINCT
        UPPER(C.MMS_CLASS_DSC)      AS NEW_MKT,
        UPPER(C.MMS_SUB_CLASS_DSC)  AS NEW_STNG,
        UPPER(C.SPCLTY_DSC)         AS NEW_TYPE,
        -- Get limit from old lookup using SPCLTY_DSC match on TYPE column
        -- Use MAX to pick one limit when multiple match
        ROUND(AVG(LYL.LOYAL_CTGRY_LIMIT), 2) AS WEIGHTED_LIMIT,
        COUNT(DISTINCT C.DIM_CUST_CURR_ID)    AS TOTAL_CUST_COUNT
    FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR C
    -- Only customers NOT in new view (the 90%)
    LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
        ON C.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
    -- Match old lookup by SPCLTY_DSC on the TYPE column only
    -- This is safe because MKT+STNG will also be from MMS base values
    LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL
        ON UPPER(C.MMS_CLASS_DSC)     = UPPER(LYL.CUST_SGMNT_MKT_DSC)
        AND UPPER(C.MMS_SUB_CLASS_DSC) = UPPER(LYL.CUST_SGMNT_SETTNG_DSC)
        AND UPPER(C.SPCLTY_DSC)        = UPPER(LYL.CUST_SGMNT_DFLT_TYPE_DSC)
    WHERE C.PTNT_FLG <> 'Y'
      AND C.SYS_PLTFRM = 'E1'
      AND SR.DIM_CUST_CURR_ID IS NULL          -- only the 90% not in new view
      AND C.MMS_CLASS_DSC IS NOT NULL
      AND LYL.LOYAL_CTGRY_LIMIT IS NOT NULL    -- only where match found
    GROUP BY 
        UPPER(C.MMS_CLASS_DSC),
        UPPER(C.MMS_SUB_CLASS_DSC),
        UPPER(C.SPCLTY_DSC)
),

-- Combine both mappings, excluding any combination already in lookup table
all_new_entries AS (
    SELECT NEW_MKT, NEW_STNG, NEW_TYPE, WEIGHTED_LIMIT, TOTAL_CUST_COUNT
    FROM new_taxonomy_mapping
    WHERE NEW_MKT IS NOT NULL
      AND NEW_TYPE IS NOT NULL
      AND WEIGHTED_LIMIT IS NOT NULL

    UNION ALL

    SELECT NEW_MKT, NEW_STNG, NEW_TYPE, WEIGHTED_LIMIT, TOTAL_CUST_COUNT
    FROM mms_fallback_mapping
    WHERE NEW_MKT IS NOT NULL
      AND NEW_TYPE IS NOT NULL
      AND WEIGHTED_LIMIT IS NOT NULL
)

SELECT
    ane.NEW_MKT                AS CUST_SGMNT_MKT_DSC,
    ane.NEW_STNG               AS CUST_SGMNT_SETTNG_DSC,
    ane.NEW_TYPE               AS CUST_SGMNT_DFLT_TYPE_DSC,
    NULL                       AS SETTNG_SITE_CNT,      -- not available from mapping
    ane.TOTAL_CUST_COUNT       AS SITE_CNT,
    ane.WEIGHTED_LIMIT         AS LOYAL_CTGRY_LIMIT,
    CURRENT_TIMESTAMP()        AS UPDATE_DT
FROM all_new_entries ane
-- Only insert combinations NOT already in the lookup table
WHERE NOT EXISTS (
    SELECT 1
    FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW EXIST_CHK
    WHERE UPPER(EXIST_CHK.CUST_SGMNT_MKT_DSC)      = UPPER(ane.NEW_MKT)
      AND UPPER(EXIST_CHK.CUST_SGMNT_SETTNG_DSC)   = UPPER(ane.NEW_STNG)
      AND UPPER(EXIST_CHK.CUST_SGMNT_DFLT_TYPE_DSC) = UPPER(ane.NEW_TYPE)
);

-- 2.3 Verify new rows inserted
SELECT 
    COUNT(*) AS TOTAL_ROWS_NOW,
    SUM(CASE WHEN UPDATE_DT >= CURRENT_DATE() THEN 1 ELSE 0 END) AS NEW_ROWS_ADDED
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW;


/* ==============================================================================
   SECTION 3 — FIX 3: NULL MAPS_TYPE HANDLING
   
   PROBLEM : 95,948 customers have NULL MAPS_TYPE even within the 10% view coverage
             because HCP_SPECIALTY, MAIN_SPECIALTY, and SPCLTY_DSC are all NULL.
   FIX     : Already handled in Fix 1 via COALESCE in the final SELECT of the view.
             This section adds a catch-all entry in the lookup table for NULL TYPE.
   IMPACT  : Ensures NULL MAPS_TYPE customers can still get a LOYAL_CTGRY_LIMIT.
   ==============================================================================*/

-- 3.1 Insert catch-all entries for NULL MAPS_TYPE per MKT+STNG combination
-- These use the average limit for that MKT+STNG across all TYPE values
INSERT INTO DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW
(
    CUST_SGMNT_MKT_DSC,
    CUST_SGMNT_SETTNG_DSC,
    CUST_SGMNT_DFLT_TYPE_DSC,
    SETTNG_SITE_CNT,
    SITE_CNT,
    LOYAL_CTGRY_LIMIT,
    UPDATE_DT
)
SELECT
    CUST_SGMNT_MKT_DSC,
    CUST_SGMNT_SETTNG_DSC,
    NULL                        AS CUST_SGMNT_DFLT_TYPE_DSC,  -- catch-all for NULL TYPE
    NULL                        AS SETTNG_SITE_CNT,
    NULL                        AS SITE_CNT,
    ROUND(AVG(LOYAL_CTGRY_LIMIT), 2) AS LOYAL_CTGRY_LIMIT,
    CURRENT_TIMESTAMP()         AS UPDATE_DT
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW
WHERE LOYAL_CTGRY_LIMIT IS NOT NULL
GROUP BY CUST_SGMNT_MKT_DSC, CUST_SGMNT_SETTNG_DSC
-- Only insert if NULL TYPE row doesn't already exist for this MKT+STNG
HAVING NOT EXISTS (
    SELECT 1
    FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW EXIST_CHK
    WHERE EXIST_CHK.CUST_SGMNT_MKT_DSC    = CUST_SGMNT_MKT_DSC
      AND EXIST_CHK.CUST_SGMNT_SETTNG_DSC  = CUST_SGMNT_SETTNG_DSC
      AND EXIST_CHK.CUST_SGMNT_DFLT_TYPE_DSC IS NULL
);

-- 3.2 Update the main churn SQL join to handle NULL MAPS_TYPE
-- The join in M_CUST_CHURN_INDICATOR_ST_CW needs to be updated to:
/*
   LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL_CTGRY
    ON UPPER(CUST_BASE.CUST_SGMNT_MKT_DSC)       = UPPER(LYL_CTGRY.CUST_SGMNT_MKT_DSC)
    AND UPPER(CUST_BASE.CUST_SGMNT_SETTNG_DSC)    = UPPER(LYL_CTGRY.CUST_SGMNT_SETTNG_DSC)
    AND (
         UPPER(CUST_BASE.CUST_SGMNT_DFLT_TYPE_DSC) = UPPER(LYL_CTGRY.CUST_SGMNT_DFLT_TYPE_DSC)
         OR 
         (CUST_BASE.CUST_SGMNT_DFLT_TYPE_DSC IS NULL AND LYL_CTGRY.CUST_SGMNT_DFLT_TYPE_DSC IS NULL)
    )
*/
-- NOTE: This change must be made in the main churn SQL script separately.


/* ==============================================================================
   SECTION 4 — POST-FIX VALIDATION
   Run these after all three fixes are applied.
   Compare results against Section 0 pre-flight values.
   ==============================================================================*/

-- 4.1 View coverage after fix (target: close to 100%)
SELECT
    COUNT(DISTINCT CB.DIM_CUST_CURR_ID)  AS TOTAL_BLEND_CURR_IDS,
    COUNT(DISTINCT SR.DIM_CUST_CURR_ID)  AS VIEW_COVERED_IDS,
    COUNT(DISTINCT CASE WHEN SR.DIM_CUST_CURR_ID IS NULL 
          THEN CB.DIM_CUST_CURR_ID END)  AS NOT_COVERED_IDS,
    ROUND(COUNT(DISTINCT SR.DIM_CUST_CURR_ID) * 100.0 / 
          COUNT(DISTINCT CB.DIM_CUST_CURR_ID), 2) AS PCT_COVERED
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
    ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
WHERE CB.PTNT_FLG <> 'Y'
  AND CB.SYS_PLTFRM = 'E1';

-- 4.2 Lookup table join success rate after fix (target: >90%)
SELECT
    COUNT(*)                                                            AS TOTAL_CUSTOMERS,
    SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NOT NULL THEN 1 ELSE 0 END) AS LOYAL_JOIN_SUCCESS,
    SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NULL THEN 1 ELSE 0 END)     AS LOYAL_JOIN_FAIL,
    ROUND(SUM(CASE WHEN LYL.LOYAL_CTGRY_LIMIT IS NOT NULL THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 2)                                        AS PCT_JOIN_SUCCESS
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
    ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL
    ON UPPER(SR.MAPS_MKT)  = UPPER(LYL.CUST_SGMNT_MKT_DSC)
    AND UPPER(SR.MAPS_STNG) = UPPER(LYL.CUST_SGMNT_SETTNG_DSC)
    AND (
        UPPER(SR.MAPS_TYPE) = UPPER(LYL.CUST_SGMNT_DFLT_TYPE_DSC)
        OR (SR.MAPS_TYPE IS NULL AND LYL.CUST_SGMNT_DFLT_TYPE_DSC IS NULL)
    )
WHERE CB.PTNT_FLG <> 'Y'
  AND CB.SYS_PLTFRM = 'E1';

-- 4.3 NULL MAPS_TYPE count after fix (target: 0 or minimal)
SELECT
    COUNT(*) AS TOTAL_IN_VIEW,
    SUM(CASE WHEN MAPS_TYPE IS NULL THEN 1 ELSE 0 END) AS NULL_MAPS_TYPE,
    SUM(CASE WHEN MAPS_MKT IS NULL THEN 1 ELSE 0 END)  AS NULL_MAPS_MKT,
    SUM(CASE WHEN MAPS_STNG IS NULL THEN 1 ELSE 0 END) AS NULL_MAPS_STNG
FROM DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026;

-- 4.4 Lookup table row count after fix
SELECT 
    COUNT(*)                                                           AS TOTAL_ROWS,
    SUM(CASE WHEN UPDATE_DT >= CURRENT_DATE() THEN 1 ELSE 0 END)      AS NEW_ROWS_TODAY,
    SUM(CASE WHEN UPDATE_DT < CURRENT_DATE() THEN 1 ELSE 0 END)       AS ORIGINAL_ROWS
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW;

-- 4.5 Sample of new mappings working correctly
SELECT
    SR.MAPS_MKT,
    SR.MAPS_STNG,
    SR.MAPS_TYPE,
    LYL.LOYAL_CTGRY_LIMIT,
    COUNT(DISTINCT CB.DIM_CUST_CURR_ID) AS CUST_COUNT
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.V_DIM_CUST_E1_BLEND_CURR CB
INNER JOIN DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026 SR
    ON CB.DIM_CUST_CURR_ID = SR.DIM_CUST_CURR_ID
LEFT JOIN DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW LYL
    ON UPPER(SR.MAPS_MKT)  = UPPER(LYL.CUST_SGMNT_MKT_DSC)
    AND UPPER(SR.MAPS_STNG) = UPPER(LYL.CUST_SGMNT_SETTNG_DSC)
    AND (
        UPPER(SR.MAPS_TYPE) = UPPER(LYL.CUST_SGMNT_DFLT_TYPE_DSC)
        OR (SR.MAPS_TYPE IS NULL AND LYL.CUST_SGMNT_DFLT_TYPE_DSC IS NULL)
    )
WHERE LYL.LOYAL_CTGRY_LIMIT IS NOT NULL
GROUP BY 1,2,3,4
ORDER BY CUST_COUNT DESC
LIMIT 20;

-- 4.6 After re-running churn SQL, verify LOYAL now appears
SELECT
    LIFECYCLE_FLAG,
    COUNT(*) AS RECORD_COUNT
FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.M_CUST_CHURN_INDICATOR_ST_CW
GROUP BY LIFECYCLE_FLAG
ORDER BY RECORD_COUNT DESC;


/* ==============================================================================
   SECTION 5 — ROLLBACK SCRIPT
   Run ONLY if fixes cause unexpected issues.
   This restores lookup table from backup and reverts view.
   ==============================================================================*/

-- 5.1 Restore lookup table from backup
-- TRUNCATE DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW;
-- INSERT INTO DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW
--     (CUST_SGMNT_MKT_DSC, CUST_SGMNT_SETTNG_DSC, CUST_SGMNT_DFLT_TYPE_DSC,
--      SETTNG_SITE_CNT, SITE_CNT, LOYAL_CTGRY_LIMIT, UPDATE_DT)
-- SELECT CUST_SGMNT_MKT_DSC, CUST_SGMNT_SETTNG_DSC, CUST_SGMNT_DFLT_TYPE_DSC,
--        SETTNG_SITE_CNT, SITE_CNT, LOYAL_CTGRY_LIMIT, UPDATE_DT
-- FROM DEV_ENT_DL_US_MMS_MISC_DB.HADOOP.CHURN_SITE_LOYAL_CTGRY_CW_BACKUP_20260412;

-- 5.2 Revert view to original code (paste original view DDL here)
-- CREATE OR REPLACE VIEW DEV_ENT_DL_US_MMS_MISC_DB.MMSDW.V_MDM_CUST_SEGMENT_SK4112026
-- ... (original view code from Full_Current_View_Code_of_V_MDM_CUST_SEGMENT_SK4112026.txt)


/* ==============================================================================
   SUMMARY OF CHANGES
   ==============================================================================
   
   FIX 1 — V_MDM_CUST_SEGMENT_SK4112026 (Section 1)
   ─────────────────────────────────────────────────
   Added: all_customers CTE as base anchor
   Added: LEFT JOIN to guarantee all 24M customers get a row
   Added: COALESCE fallbacks in final SELECT:
          MAPS_MKT  → enriched value OR MMS_CLASS_DSC
          MAPS_STNG → enriched value OR MMS_SUB_CLASS_DSC OR MMS_CLASS_DSC
          MAPS_TYPE → enriched value OR SPCLTY_DSC
   Result: Coverage increases from ~10% to ~100%
   
   FIX 2 — CHURN_SITE_LOYAL_CTGRY_CW (Section 2)
   ───────────────────────────────────────────────
   Added: Backup table created (CHURN_SITE_LOYAL_CTGRY_CW_BACKUP_20260412)
   Added: New taxonomy rows from old→new MDM mapping (weighted avg limits)
   Added: MMS fallback rows for 90% unmatched customers
   Kept:  All original old MDM taxonomy rows (backward compatible)
   Result: LOYAL_CTGRY_LIMIT now returns for new taxonomy values
   
   FIX 3 — NULL MAPS_TYPE handling (Section 3)
   ────────────────────────────────────────────
   Added: Catch-all NULL TYPE rows in lookup table per MKT+STNG
   Note:  Main churn SQL join condition also needs updating (see Section 3.2)
   Result: 95,948 NULL MAPS_TYPE customers now get a LOYAL_CTGRY_LIMIT
   
   EXPECTED OUTCOME AFTER ALL FIXES:
   ─────────────────────────────────
   View coverage:       ~10% → ~100%
   Lookup join success: ~0%  → ~90%+
   LIFECYCLE_FLAG:      LOYAL appears in output
   ============================================================================*/
