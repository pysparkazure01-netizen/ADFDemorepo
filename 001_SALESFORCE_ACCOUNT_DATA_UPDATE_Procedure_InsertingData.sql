CREATE OR REPLACE PROCEDURE PROD_MARKETING_WAREHOUSE.MARKETING_REPORTS.SALESFORCE_ACCOUNT_MARKETING_FIELDS_CT_UPDATE()

RETURNS VARCHAR

LANGUAGE SQL

EXECUTE AS OWNER

AS '

DECLARE

 

              VAR_ZDIG_APP_CODE STRING;

              VAR_TRACKING_TABLE_NAME STRING;

              VAR_TARGET_TABLE_NAME STRING;

              VAR_SUCCESS_MESSAGE STRING;

              VAR_TRKG_INDC STRING;

              ZDIG_QUERY STRING;

              VAR_SRC_UNIQ_CD STRING;

              VAR_APP_PERSONA_APPEND STRING;

              VAR_APP_PERSONA_DETAIL STRING;

   

BEGIN

 

VAR_SUCCESS_MESSAGE := ''SUCCESSFULLY COMLPETED'';

 

insert into PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_DATA_UPDATE

select PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_DATA_UPDATE_ID.nextval as SALESFORCE_ACCOUNT_DATA_UPDATE_ID,

b.COMPANY_ID as SalesforceAccountId,

b.MKT_WEBSITE as MKT_Analytics_Website,

b.MKT_ENRICHMENT_ID,

b.MKT_ACCOUNT_PARENT_VERT,

b.MKT_ACCOUNT_VERT,

b.MKT_RELEVANT,

b.MKT_SEGMENTATION_1,

b.MKT_SEGMENTATION_2,

b.MKT_SEGMENTATION_3,

b.MKT_SEGMENTATION_4,

b.MKT_PREDICTION_1,

b.MKT_PREDICTION_2,

b.MKT_PREDICTION_3,

b.MKT_PREDICTION_4,

b.MKT_CSAT,

b.MKT_NPS,

getdate() as ROW_CRE_DT,

''MARKETING_DEVELOPER'' as ROW_CRE_USR_ID

from PROD_MARKETING_WAREHOUSE.marketing_reports.company_matrix b inner join

prod_datalake.salesforce.account a on b.COMPANY_ID = a.id

where b.MKT_RELEVANT = 1

and (nvl(to_varchar(b.mkt_website),'''') <> nvl(a.mkt_analytics_website__c,'''') or

nvl(to_varchar(b.MKT_ENRICHMENT_ID),'''') <> nvl(a.mkt_enrichment_id__c,'''') or

nvl(to_varchar(b.MKT_ACCOUNT_PARENT_VERT),'''') <> nvl(a.mkt_account_parent_vert__c,'''') or

nvl(to_varchar(b.MKT_ACCOUNT_VERT),'''') <> nvl(a.mkt_account_vert__c,'''') or

nvl(to_varchar(b.MKT_RELEVANT),'''') <> nvl(a.mkt_relevant__c,'''') or

nvl(to_varchar(b.MKT_SEGMENTATION_1),'''') <> nvl(a.mkt_segmentation_1__c,'''') or

nvl(to_varchar(b.MKT_SEGMENTATION_2),'''') <> nvl(a.mkt_segmentation_2__c,'''') or

nvl(to_varchar(b.MKT_SEGMENTATION_3),'''') <> nvl(a.mkt_segmentation_3__c,'''') or

nvl(to_varchar(b.MKT_SEGMENTATION_4),'''') <> nvl(a.mkt_segmentation_4__c,'''') or

nvl(to_varchar(b.MKT_PREDICTION_1),'''') <> nvl(a.mkt_prediction_1__c,'''') or

nvl(to_varchar(b.MKT_PREDICTION_2),'''') <> nvl(a.mkt_prediction_2__c,'''') or

nvl(to_varchar(b.MKT_PREDICTION_3),'''') <> nvl(a.mkt_prediction_3__c,'''') or

nvl(to_varchar(b.MKT_PREDICTION_4),'''') <> nvl(a.mkt_prediction_4__c,'''') or

nvl(to_varchar(b.MKT_CSAT),'''') <> nvl(a.mkt_csat__c,'''') or

nvl(to_varchar(b.MKT_NPS),'''') <> nvl(a.mkt_nps__c,''''));

 

insert into PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_DATA_UPDATE

select PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.SALESFORCE_ACCOUNT_DATA_UPDATE_ID.nextval as SALESFORCE_ACCOUNT_DATA_UPDATE_ID,

b.COMPANY_ID as SalesforceAccountId,

b.MKT_WEBSITE as MKT_Analytics_Website,

b.MKT_ENRICHMENT_ID,

b.MKT_ACCOUNT_PARENT_VERT,

b.MKT_ACCOUNT_VERT,

b.MKT_RELEVANT,

b.MKT_SEGMENTATION_1,

b.MKT_SEGMENTATION_2,

b.MKT_SEGMENTATION_3,

b.MKT_SEGMENTATION_4,

b.MKT_PREDICTION_1,

b.MKT_PREDICTION_2,

b.MKT_PREDICTION_3,

b.MKT_PREDICTION_4,

b.MKT_CSAT,

b.MKT_NPS,

getdate() as ROW_CRE_DT,

''MARKETING_DEVELOPER'' as ROW_CRE_USR_ID

from PROD_MARKETING_WAREHOUSE.marketing_reports.company_matrix b inner join

prod_datalake.salesforce.account a on b.COMPANY_ID = a.id

where b.MKT_RELEVANT = ''0'' and nvl(a.mkt_relevant__c,''0'') = ''1'';

 

call PROD_MARKETING_WAREHOUSE.DWNSTRM_SALESFORCE.MARKETING_SALESFORCE_ACCOUNT_UPDATE();

 

RETURN VAR_SUCCESS_MESSAGE;

 

 

END;

';

 

 

CREATE OR REPLACE PROCEDURE PROD_MARKETING_WAREHOUSE.MARKETING_REPORTS.SALESFORCE_CONTACT_MARKETING_FIELDS_CT_UPDATE()

RETURNS VARCHAR

LANGUAGE JAVASCRIPT

EXECUTE AS CALLER

AS '

  try {

    return ''This is a dummy procedure'';

  } catch(err) {

    return ''Error: '' + err.message;

  }

';