
'''This is a config file for Funnel Analytics data processing. This is a container for static parameters and executable strings.
 This helps to make the calling python class simple and arranged like maintaining a load order
Author: BI Reporting Team(bireporting....)

Please contact Dave W(david.............),
Bharath N...(bharath...............),
or
Krishna M... (krishna..................)
before changing any code
'''





CHECK_IF_ALREADY_LOADED='''select * from SALESFORCE_KT_PROD.%s  
where PERIOD_NAME='%s'
AND WORKDAY_INFO='%s'
LIMIT 1'''



'''Insert queries goes here. snapshot date, period name and workday info are capured to be replaced at run time'''

'''LOAD TABLE ALL_OPPORTUNITIES'''
STMNT1='''insert into ALL_OPPORTUNITIES
          (OPPTY_ID,OPPTY_NUMBER, OPPTY_NAME,Close_Date,CREATED_DATE, OPPTY_CREATIONBOOKED_TAT, SALES_STAGE,PARTNER_ACCOUNT_NAME, OPPTY_CREATOR_FULL_NAME,OPPTY_CREATOR_ROLE, OPPORTUNITY_TYPE, OPPORTUNITY_SUB_TYPE, LEAD_SOURCE, LEAD_NUM, REVENUE,OPPORTUNITY_SPLIT_AMOUNT, SPLIT_PERCENT,INDUSTRY, INDUSTRY_SEGMENT, INDUSTRY_SUB_SEGMENT, CUSTOMER_PO_NUMBER, BOOKED_ORDER_NUMBER, ACCOUNT_NAME, ACCOUNTID, OCN, OSN, PARENT_PARTY, PARENT_PARTY_NUMBER, CONTACT_NAME, FE_CODE, SPLIT_FE_CODE, LOST_CANCELLED_REASON, PRIMARY_PRODUCT, PRIMARY_PL, QUOTE_NUMBER, QUOTE_REVISION, OPPTY_HIER_ID, SPLIT_HIER_ID, SPLIT_OWNER_ID, TEAM_MEMBER, MEMBER_ROLE,SPLIT_CREATEDDATE,CLOSE_MONTH,SPLIT_ID,OPPTY_CREATOR_ROLE_NAME,SPLIT_FE_CODE_REGION,ACCOUNT_COUNTRY,Challenger_skills,CHALLENGER_L_AND_R,SNAPSHOT_DT,PERIOD_NAME,WORKDAY_INFO)

SELECT 
Oppty.Id as Oppty_Id	,
Oppty.Opportunity_Name_Auto_Number__c as Oppty_Number	,
Oppty.Name as Oppty_Name	,
Oppty.CLOSEDATE as Close_Date	,
Oppty.CreatedDate as Created_Date	,
Oppty.Oppty_creation_to_Order_booked_TAT__c as Oppty_creation_to_Order_booked_TAT	,
Oppty.StageName as Sales_Stage	,
Partner.Name as Partner_Account_Name	,
Usr.Name as Oppty_Creator_Full_Name	,
UsrRole.RollupDescription as Oppty_Creator_Role	,
Oppty.Type as Opportunity_Type	,
Oppty.Opportunity_Sub_Type__c as Opportunity_Sub_Type	,
Oppty.LeadSource as Lead_Source	,
Oppty.Lead_Number__c as Lead_Num	,
Oppty.Amount as Revenue	,
OpptyS.SplitAmount as Opportunity_Split_Amount	,
OpptyS.SplitPercentage as Split_Percent	,
Oppty.Industry__c as Industry	,
Oppty.Industry_Segment__c as Industry_Segment	,
Oppty.Industry_Sub_Segment__c as Industry_Sub_Segment	,
Oppty.Purchase_Order__c as Customer_PO_Number	,
Oppty.Booked_Order__c as Booked_Order_Number	,
Acc.Name as Account_Name	,
Oppty.AccountId	,
Acc.OCN__c as OCN	,
Acc.OSN__c as OSN	,
Acc.Parent_Party__c as Parent_Party	,
Acc.Parent_Site__c as Parent_Party_Number	,
Con.Name as Contact_Name	,
FE_C.Name as FE_Code	,
OS_FE_C.Name as Split_FE_Code	,
Oppty.Lost_Reason__c as Lost_Cancelled_Reason	,
Oppty.Product_Name__c as Primary_Product	,
Oppty.Product_line__c as Primary_PL	,
QP.Name as Quote_Number	,
QP.Revision__c as Quote_Revision	,
Oppty.Reporting_Hierachy__c as Oppty_Hier_Id	,
OpptyS.Reporting_Hierarchy__c as Split_Hier_Id	,
OpptyS.SplitOwnerId as Split_Owner_Id	,
OpptyS.Team_member__c as Team_Member	,
OpptyT.TeamMemberRole as Member_Role	,
OpptyS.CreatedDate as Split_CreatedDate	,
Oppty.Close_Month__c as Close_Month	,
OpptyS.Id as Split_Id	,
Oppty.Challenger_skills_applied__c as Challenger_skills	,
OS_FE_C.Name as Split_FE_Code	,
Acc.GlobalCountry__c as Account_Country	,
OS_FE_C.Region__c as Split_FE_Code_Region	,
Oppty.Challenger_learning_and_results__c as Challenger_l_and_r	,

current_date,
'%s',
'%s'

FROM Opportunity Oppty
LEFT OUTER JOIN OpportunitySplit OpptyS ON Oppty.ID = OpptyS.OPPORTUNITYID
LEFT OUTER JOIN OpportunitySplitType OpptyST ON OpptyS.SPLITTYPEID = OpptyST.ID
LEFT OUTER JOIN OpportunityTeamMember OpptyT ON OpptyS.SplitOwnerId = OpptyT.UserId AND OpptyS.OpportunityId = OpptyT.OpportunityId
LEFT OUTER JOIN FE_Code__c OS_FE_C ON OpptyT.FE_Code_Lookup__c = OS_FE_C.ID 

LEFT OUTER JOIN Account Acc ON Oppty.AccountId = Acc.ID
LEFT OUTER JOIN Account Partner ON Oppty.Partner_Account__c = Partner.ID
LEFT OUTER JOIN Contact Con ON Oppty.Contact__c = Con.ID

LEFT OUTER JOIN User Usr ON Oppty.CreatedById = Usr.ID
LEFT OUTER JOIN UserRole UsrRole ON Usr.UserRoleId = UsrRole.ID
LEFT OUTER JOIN Address__c Addr ON Oppty.Quote_To_Address__c = Addr.ID
LEFT OUTER JOIN FE_Code__c FE_C ON Oppty.FE_Code__c = FE_C.ID 

LEFT OUTER JOIN (SELECT 
Q.Name, Q.Revision__c,Q.BigMachines__Opportunity__c
FROM BigMachines__Quote__c Q
where Q.BigMachines__Is_Primary__c = 'true') QP ON QP.BigMachines__Opportunity__c = Oppty.ID

'''

'''LOAD TABLE OPPORTUNITY_SNAPSHOT'''

STMNT2='''insert into  SALESFORCE_KT_PROD.opportunity_snapshot( 

Oppty_Id,
Oppty_Number,
Oppty_Name,
Close_Date,
Created_Date,
OPPTY_CREATIONBOOKED_TAT,
Sales_Stage,
Partner_Account_Name,
Oppty_Creator_Full_Name,
Oppty_Creator_Role,
Oppty_Creator_Role_Name,
Opportunity_Type,
Opportunity_Sub_Type,
Lead_Source,
Lead_Num,
Revenue,
Opportunity_Split_Amount,
Split_Percent,
Industry,
Industry_Segment,
Industry_Sub_Segment,
Customer_PO_Number,
Booked_Order_Number,
Account_Name,
AccountId,
OCN,
OSN,
Parent_Party,
Parent_Party_Number,
Account_Country,
Contact_Name,
FE_Code,
Split_FE_Code,
Split_FE_Code_Region,
Lost_Cancelled_Reason,
Primary_Product,
Primary_PL,
Quote_Number,
Quote_Revision,
Oppty_Hier_Id,
Split_Hier_Id,
Split_Owner_Id,
Team_Member,
Member_Role,
Split_CreatedDate,
Close_Month,
Split_Id,
Challenger_l_and_r,
Challenger_skills,
SNAPSHOT_DT,
PERIOD_NAME,
WORKDAY_INFO)

SELECT 
Oppty.Id as Oppty_Id,
Oppty.Opportunity_Name_Auto_Number__c as Oppty_Number,
Oppty.Name as Oppty_Name,
Oppty.CLOSEDATE as Close_Date,
Oppty.CreatedDate as Created_Date,
Oppty.Oppty_creation_to_Order_booked_TAT__c as OPPTY_CREATIONBOOKED_TAT,
Oppty.StageName as Sales_Stage,
Partner.Name as Partner_Account_Name,
Usr.Name as Oppty_Creator_Full_Name,
UsrRole.RollupDescription as Oppty_Creator_Role,
UsrRole.DeveloperName as Oppty_Creator_Role_Name,
Oppty.Type as Opportunity_Type,
Oppty.Opportunity_Sub_Type__c as Opportunity_Sub_Type,
Oppty.LeadSource as Lead_Source,
Oppty.Lead_Number__c as Lead_Num,
Oppty.Amount as Revenue,
OpptyS.SplitAmount as Opportunity_Split_Amount,
OpptyS.SplitPercentage as Split_Percent,
Oppty.Industry__c as Industry,
Oppty.Industry_Segment__c as Industry_Segment,
Oppty.Industry_Sub_Segment__c as Industry_Sub_Segment,
Oppty.Purchase_Order__c as Customer_PO_Number,
Oppty.Booked_Order__c as Booked_Order_Number,
Acc.Name as Account_Name,
Oppty.AccountId,
Acc.OCN__c as OCN,
Acc.OSN__c as OSN,
Acc.Parent_Party__c as Parent_Party,
Acc.Parent_Site__c as Parent_Party_Number,
Acc.GlobalCountry__c as Account_Country,
Con.Name as Contact_Name,
FE_C.Name as FE_Code,
OS_FE_C.Name as Split_FE_Code,
OS_FE_C.Region__c as Split_FE_Code_Region,
Oppty.Lost_Reason__c as Lost_Cancelled_Reason,
Oppty.Product_Name__c as Primary_Product,
Oppty.Product_line__c as Primary_PL,
QP.Name as Quote_Number,
QP.Revision__c as Quote_Revision,
Oppty.Reporting_Hierachy__c as Oppty_Hier_Id,
OpptyS.Reporting_Hierarchy__c as Split_Hier_Id,
OpptyS.SplitOwnerId as Split_Owner_Id,
OpptyS.Team_member__c as Team_Member,
OpptyT.TeamMemberRole as Member_Role,
OpptyS.CreatedDate as Split_CreatedDate,
Oppty.Close_Month__c as Close_Month,
OpptyS.Id as Split_Id,
Oppty.Challenger_learning_and_results__c as Challenger_l_and_r,
Oppty.Challenger_skills_applied__c as Challenger_skills,
current_date,
'%s',
'%s'
FROM Opportunity Oppty
LEFT OUTER JOIN OpportunitySplit OpptyS ON Oppty.ID = OpptyS.OPPORTUNITYID
LEFT OUTER JOIN OpportunitySplitType OpptyST ON OpptyS.SPLITTYPEID = OpptyST.ID
LEFT OUTER JOIN OpportunityTeamMember OpptyT ON OpptyS.SplitOwnerId = OpptyT.UserId AND OpptyS.OpportunityId = OpptyT.OpportunityId
LEFT OUTER JOIN FE_Code__c OS_FE_C ON OpptyT.FE_Code_Lookup__c = OS_FE_C.ID 

LEFT OUTER JOIN Account Acc ON Oppty.AccountId = Acc.ID
LEFT OUTER JOIN Account Partner ON Oppty.Partner_Account__c = Partner.ID
LEFT OUTER JOIN Contact Con ON Oppty.Contact__c = Con.ID

LEFT OUTER JOIN User Usr ON Oppty.CreatedById = Usr.ID
LEFT OUTER JOIN UserRole UsrRole ON Usr.UserRoleId = UsrRole.ID
LEFT OUTER JOIN Address__c Addr ON Oppty.Quote_To_Address__c = Addr.ID
LEFT OUTER JOIN FE_Code__c FE_C ON Oppty.FE_Code__c = FE_C.ID 

LEFT OUTER JOIN (SELECT 
Q.Name, Q.Revision__c,Q.BigMachines__Opportunity__c
FROM BigMachines__Quote__c Q
where Q.BigMachines__Is_Primary__c = 'true') QP ON QP.BigMachines__Opportunity__c = Oppty.ID '''

''' Process data for opportunity_snapshot_daily'''

STMNT3='''insert into  SALESFORCE_KT_PROD.opportunity_snapshot_daily( 

Oppty_Id,
Oppty_Number,
Oppty_Name,
Close_Date,
Created_Date,
OPPTY_CREATIONBOOKED_TAT,
Sales_Stage,
Partner_Account_Name,
Oppty_Creator_Full_Name,
Oppty_Creator_Role,
Oppty_Creator_Role_Name,
Opportunity_Type,
Opportunity_Sub_Type,
Lead_Source,
Lead_Num,
Revenue,
Opportunity_Split_Amount,
Split_Percent,
Industry,
Industry_Segment,
Industry_Sub_Segment,
Customer_PO_Number,
Booked_Order_Number,
Account_Name,
AccountId,
OCN,
OSN,
Parent_Party,
Parent_Party_Number,
Account_Country,
Contact_Name,
FE_Code,
Split_FE_Code,
Split_FE_Code_Region,
Lost_Cancelled_Reason,
Primary_Product,
Primary_PL,
Quote_Number,
Quote_Revision,
Oppty_Hier_Id,
Split_Hier_Id,
Split_Owner_Id,
Team_Member,
Member_Role,
Split_CreatedDate,
Close_Month,
Split_Id,
Challenger_l_and_r,
Challenger_skills,
SNAPSHOT_DT,
PERIOD_NAME,
WORKDAY_INFO)

SELECT 
Oppty.Id as Oppty_Id,
Oppty.Opportunity_Name_Auto_Number__c as Oppty_Number,
Oppty.Name as Oppty_Name,
Oppty.CLOSEDATE as Close_Date,
Oppty.CreatedDate as Created_Date,
Oppty.Oppty_creation_to_Order_booked_TAT__c as OPPTY_CREATIONBOOKED_TAT,
Oppty.StageName as Sales_Stage,
Partner.Name as Partner_Account_Name,
Usr.Name as Oppty_Creator_Full_Name,
UsrRole.RollupDescription as Oppty_Creator_Role,
UsrRole.DeveloperName as Oppty_Creator_Role_Name,
Oppty.Type as Opportunity_Type,
Oppty.Opportunity_Sub_Type__c as Opportunity_Sub_Type,
Oppty.LeadSource as Lead_Source,
Oppty.Lead_Number__c as Lead_Num,
Oppty.Amount as Revenue,
OpptyS.SplitAmount as Opportunity_Split_Amount,
OpptyS.SplitPercentage as Split_Percent,
Oppty.Industry__c as Industry,
Oppty.Industry_Segment__c as Industry_Segment,
Oppty.Industry_Sub_Segment__c as Industry_Sub_Segment,
Oppty.Purchase_Order__c as Customer_PO_Number,
Oppty.Booked_Order__c as Booked_Order_Number,
Acc.Name as Account_Name,
Oppty.AccountId,
Acc.OCN__c as OCN,
Acc.OSN__c as OSN,
Acc.Parent_Party__c as Parent_Party,
Acc.Parent_Site__c as Parent_Party_Number,
Acc.GlobalCountry__c as Account_Country,
Con.Name as Contact_Name,
FE_C.Name as FE_Code,
OS_FE_C.Name as Split_FE_Code,
OS_FE_C.Region__c as Split_FE_Code_Region,
Oppty.Lost_Reason__c as Lost_Cancelled_Reason,
Oppty.Product_Name__c as Primary_Product,
Oppty.Product_line__c as Primary_PL,
QP.Name as Quote_Number,
QP.Revision__c as Quote_Revision,
Oppty.Reporting_Hierachy__c as Oppty_Hier_Id,
OpptyS.Reporting_Hierarchy__c as Split_Hier_Id,
OpptyS.SplitOwnerId as Split_Owner_Id,
OpptyS.Team_member__c as Team_Member,
OpptyT.TeamMemberRole as Member_Role,
OpptyS.CreatedDate as Split_CreatedDate,
Oppty.Close_Month__c as Close_Month,
OpptyS.Id as Split_Id,
Oppty.Challenger_learning_and_results__c as Challenger_l_and_r,
Oppty.Challenger_skills_applied__c as Challenger_skills,
current_date,
'%s',
'%s'
FROM Opportunity Oppty
LEFT OUTER JOIN OpportunitySplit OpptyS ON Oppty.ID = OpptyS.OPPORTUNITYID
LEFT OUTER JOIN OpportunitySplitType OpptyST ON OpptyS.SPLITTYPEID = OpptyST.ID
LEFT OUTER JOIN OpportunityTeamMember OpptyT ON OpptyS.SplitOwnerId = OpptyT.UserId AND OpptyS.OpportunityId = OpptyT.OpportunityId
LEFT OUTER JOIN FE_Code__c OS_FE_C ON OpptyT.FE_Code_Lookup__c = OS_FE_C.ID 

LEFT OUTER JOIN Account Acc ON Oppty.AccountId = Acc.ID
LEFT OUTER JOIN Account Partner ON Oppty.Partner_Account__c = Partner.ID
LEFT OUTER JOIN Contact Con ON Oppty.Contact__c = Con.ID

LEFT OUTER JOIN User Usr ON Oppty.CreatedById = Usr.ID
LEFT OUTER JOIN UserRole UsrRole ON Usr.UserRoleId = UsrRole.ID
LEFT OUTER JOIN Address__c Addr ON Oppty.Quote_To_Address__c = Addr.ID
LEFT OUTER JOIN FE_Code__c FE_C ON Oppty.FE_Code__c = FE_C.ID 

LEFT OUTER JOIN (SELECT 
Q.Name, Q.Revision__c,Q.BigMachines__Opportunity__c
FROM BigMachines__Quote__c Q
where Q.BigMachines__Is_Primary__c = 'true') QP ON QP.BigMachines__Opportunity__c = Oppty.ID '''

STMNT4='''insert into Oppty_Snapshot_asis
          (ID, 
ISDELETED, 
ACCNTID, 
RECORDTYPEID, 
ISPRIVATE, 
OPPTYNAME, 
DESCRIPTION, 
STAGENAME, 
AMOUNT, 
PROBABILITY, 
EXPECTEDREV, 
TOTALOPPTYQTY, 
CLOSEDATE, 
TYPE, 
NEXTSTEP, 
LEADSOURCE, 
ISCLOSED, 
ISWON, 
FORECASTCAT, 
FORECASTCATNAME, 
CAMPAIGNID, 
HASOPPTYLINEITEM, 
ISSPLIT, 
PRICEBOOK2ID, 
OWNERID, 

ISEXFROMTERR2FILT, 
CREATEDDATE, 
CREATEDBYID, 
LASTMODIFIEDDATE, 
LASTMODIFIEDBYID, 
SYSTEMMODSTAMP, 
LASTACTIVITYDATE, 
FISCALQUARTER, 
FISCALYEAR, 
FISCAL, 
LASTVIEWEDDATE, 
LASTREFERENCEDDATE, 
 
 
HASOPENACTIVITY, 
HASOVERDUETASK, 
BIGMACHINES__LINE_ITEMS, 
ACCNT_ADDR, 
ACCNT_PARENT_SITE, 
ACCNT_PROFILE_CLASS, 
ACNT_NUMBER_ACC_ON_OPTY, 
ALT_ACCNT_NAME, 
AMOUNT_WON, 
APPROVAL_STATUS, 
BOOKED_ORDER_AMOUNT_LOCAL, 
BOOKED_ORDER_AMOUNT, 
BOOKED_ORDER, 
BOOKED_ORDER_STATUS, 
BUDGET_AMOUNT, 

BUD_APPRD_BUDGET_PROC_ALGNED, 

CLOSE_MONTH, 
CLOSE_PUSH_PULL, 
COMMIT_AMOUNT, 
COMPR_ROLE_ID_LOG_OWNER_PRNT, 
COMPETITION, 
CONF_NEED_TIME_BUDGET, 

CONTACT_DEPARTMENT, 
CONTACT_NAME_ALT, 
CONTACT_PHONE, 
CONTACT, 
CON_NUM_CON_OPPTY, 
CREATED_BY_NAME, 
CRIT_ISSUES_TO_RESLV, 
CUST_PO_NUMBER, 
CUST_SUBMIT_DATE, 
CUST_HAS_RES_ASSIGNED, 
CUST_PROV_VERBAL_COM_PO, 
DEAL_PLAN_ITA, 
 
DESIRED_END_STATE, 
DISCOVERY_DATE, 
DISCOVERY_TO_PROPOSAL, 

END_CUST_ACCNT, 
END_CUST_ADDR_DTL, 

--END_CUST_CITY, 
--ENGAGEMENT_NOTES, 
ESTIMATED_CLOSE_DATE, 
FE_CODE, 
--FE_CODE_FOR_CPQ_TEXT, 
FINANCIAL_TERMS_TYPE, 
--FIRST_NAME, 
GUT_FEEL_USD, 
GUT_FEEL, 
HDR_UPGR_MODEL_SN, 
INDUSTRY_SEGMENT, 
INDUSTRY_SUB_SEGMENT, 
INDUSTRY, 
ISCHANGEDFROMCPQ, 
ISEXCLUDEDFROMASSIGN, 
JP_KATAKANA, 
KEY_DECISION_CRITERIA, 
KEYSIGHT_ORDER_NUMBER, 
KEYSIGHT_FAVORED, 
--KEYSIGHT_POTENTIAL_FIT, 
--KEYSIGHT_VALUE_PROP_ROI, 
--LAST_NAME, 
LEAD_NUMBER, 
--LOCAL_CURR_CODE, 
LOG_IN_USER_ROLE_ID, 
--LOST_DATE, 
LOST_REASON, 
NEGOTIATION_DATE, 
--NEG_LIKELIHD_COMPLXTY, 
--NEXT_STEPS, 
NEXT_STEPS_IST_OR_CUST, 
--NON_DISC_AGREEMENT, 
OIC_FE_CODE1, 
OPPTY_ACCNT_REGION, 
OPPTY_ID, 
OPPTY_NAME_AUTO_NUMBER, 
OPPTY_NAME, 
OPPTY_PRODUCT_FROM_LEAD, 
OPPTY_SUB_TYPE, 
--OPPTY_E2E_TAT, 
OPPTY_NAME_FORMULA, 
OPPTY_CR_TO_ORDER_BKD_TAT, 
--OPTY_TO_ORDER_TAT, 
ORDER_BOOKED_DATE, 
ORDER_CANCEL_FLAG, 
ORDER_PENDING_DATE, 
ORDER_STATUS, 
PARTNER_ACCNT, 
POWER_OF_1, 
--PRIMARY_PRODUCT, 
PRODUCT_NAME, 
PROPOSAL_DATE, 
PROPOSAL_GEN_SENT, 
PROPOSAL_TO_NEG, 
PURCHASE_ORDER, 
QUOTE_CURR_AMOUNT, 
QUOTE_CURR, 
QUOTE_SENT, 
QUOTE_TO_ADDR, 
QUOTE_AVAILABLE, 
QUOTE_TO_ADDR_DTLS, 
RECORD_LOCKED, 
--REJECTION_REASON, 
RENEWAL, 
REV_USD, 
REV, 
--SALES_CHANNEL, 
--SALES_LEAD_STATUS, 
QUOTA_TEAM_FE_CODE, 
SOFT_WARNING_ACKNOWLEDGE, 
--SOLUTION_PARTNERS_MFG_REPS, 
SOURCE_LEAD, 
STMNT_OF_WORK_SOW, 
STRATEGY_TO_WIN, 
--TECHNICAL_SPECS, 
--TIMEFRAME_NOTES, 
TIMES_CLOSE_DATE_CHANGED, 
--X3RD_PARTY_VET_FOR_INCL, 
CONF_PROP_AND_AS_FINALIST, 
PROBLEM_DEC_CR_UNDRSTD, 
VALUE_PROPOSITION_ROI, 
FIRST_FROM_TO_STAGE, 
FIRST_STAGE_REV_DATE_TIME, 
LAST_FROM_TO_STAGE, 
LAST_STAGE_REV_DATE_TIME, 
NUM_OF_STAGE_REVERSAL, 
Q4_REV_READY, 
LEAD_NUM, 
IS_COPY_QUOTE, 
OCN, 
ACCNT_CITY, 
ACCNT_COUNTRY, 
ACCNT_STATE, 
PRODUCT_LINE, 
RMU, 
REPORTING_HIERACHY, 
CHALLENGER_LRNG_AND_RSLT, 
CHALLENGER_SKILLS_APPLIED,
SNAPSHOT_DT,
PERIOD_NAME,
WORKDAY_INFO
)
          SELECT
    id,
    isdeleted,
    accountid,
    recordtypeid,
    isprivate,
    name,
    description,
    stagename,
    amount,
    probability,
    expectedrevenue,
    totalopportunityquantity,
    TO_CHAR(closedate,'DD-MON-YYYY'),
    type,
    nextstep,
    leadsource,
    isclosed,
    iswon,
    forecastcategory,
    forecastcategoryname,
    campaignid,
    hasopportunitylineitem,
    issplit,
    pricebook2id,
    ownerid,
    --territory2id,
    isexcludedfromterritory2filter,
    TO_CHAR(createddate,'DD-MON-YYYY'),
    createdbyid,
    TO_CHAR(lastmodifieddate,'DD-MON-YYYY'),
    lastmodifiedbyid,
    systemmodstamp,
    TO_CHAR(lastactivitydate,'DD-MON-YYYY'),
    fiscalquarter,
    fiscalyear,
    fiscal,
    TO_CHAR(lastvieweddate,'DD-MON-YYYY'),
    TO_CHAR(lastreferenceddate,'DD-MON-YYYY'),
   -- syncedquoteid,
  --  contractid,
    hasopenactivity,
    hasoverduetask,
    bigmachines__line_items__c,
    account_address__c,
    account_parent_site__c,
    account_profile_class__c,
    account_number_of_the_account_on_opty__c,
    alt_account_name__c,
    amount_won__c,
    approval_status__c,
    booked_order_amount_local__c,
    booked_order_amount__c,
    booked_order__c,
    booked_order_status__c,
    budget_amount__c,
   -- budget__c,
    budget_approved_budget_process_aligned__c,
  --  case__c,
    close_month__c,
    close_push_pull__c,
    commit_amount__c,
    compare_role_id_logged_in_owner_parent__c,
    competition__c,
    confirmed_need_timeframe_budget__c,
    --contact_department_alternate__c,
    contact_department__c,
    contact_name_alt__c,
    contact_phone__c,
    contact__c,
    contact_number_of_the_contact_on_oppty__c,
    created_by_name__c,
    critical_issues_barriers_to_resolve__c,
    customer_po_number__c,
    TO_CHAR(customer_submit_date__c,'DD-MON-YYYY'),
    customer_has_resources_assigned__c,
    customer_provided_verbal_commitment_po__c,
    deal_plan_ita__c,
    --design_flow_environment__c,
    desired_end_state__c,
    TO_CHAR(discovery_date__c,'DD-MON-YYYY'),
    discovery_to_proposal__c,
   -- dongle__c,
    end_customer_account__c,
    end_customer_address_detail__c,
   -- end_customer_city__c,
   -- engagement_notes__c,
    TO_CHAR(estimated_close_date__c,'DD-MON-YYYY'),
    fe_code__c,
   -- fe_code_for_cpq_text__c,
    financial_terms_type__c,
    --first_name__c,
    gut_feel_usd__c,
    gut_feel__c,
    hardware_upgrade_model_sn__c,
    industry_segment__c,
    industry_sub_segment__c,
    industry__c,
    ischangedfromcpq__c,
    isexcludedfromassign__c,
    jp_katakana__c,
    key_decision_criteria__c,
    keysight_order_number__c,
    keysight_favored__c,
    --keysight_potential_fit__c,
    --keysight_value_proposition_roi__c,
   -- last_name__c,
    lead_number__c,
    --local_currency_code__c,
    logged_in_user_role_id__c,
   -- TO_CHAR(lost_date__c,'DD_MON-YYYY'),
    lost_reason__c,
    TO_CHAR(negotiation_date__c,'DD-MON-YYYY'),
    --negotiation_likelihood_complexity__c,
   -- next_steps__c,
    next_steps_by_sales_ist_or_customer__c,
   -- non_disc_agreement__c,
    oic_fe_code1__c,
    opportunity_account_region__c,
    opportunity_id__c,
    opportunity_name_auto_number__c,
    opportunity_name__c,
    opportunity_product_from_lead__c,
    opportunity_sub_type__c,
   -- oppty_e2e_tat__c,
    oppty_name_formula__c,
    oppty_creation_to_order_booked_tat__c,
    --opty_to_order_tat__c,
    TO_CHAR(order_booked_date__c,'DD-MON-YYYY'),
    order_cancel_flag__c,
    TO_CHAR(order_pending_date__c,'DD-MON-YYYY'),
    order_status__c,
    partner_account__c,
    power_of_1__c,
   -- primary_product__c,
    product_name__c,
    TO_CHAR(proposal_date__c,'DD-MON-YYYY'),
    proposal_generated_sent__c,
    proposal_to_negotiation__c,
    purchase_order__c,
    quote_currency_amount__c,
    quote_currency__c,
    quote_sent__c,
    quote_to_address__c,
    quote_available__c,
    quote_to_address_details__c,
    record_locked__c,
    --rejection_reason__c,
    renewal__c,
    revenue_usd__c,
    revenue__c,
    --sales_channel__c,
    --sales_lead_status__c,
    quota_team_fe_code__c,
    soft_warning_acknowledge__c,
   -- solution_partners_mfg_reps__c,
    source_lead__c,
    statement_of_work_sow__c,
    strategy_to_win__c,
    --technical_specs__c,
    --timeframe_notes__c,
    times_close_date_has_changed__c,
   -- x3rd_party_vetted_for_inclusion__c,
    confirmed_proposal_and_as_finalist__c,
    problem_decision_criteria_understood__c,
    value_proposition_roi__c,
    first_from_to_stage__c,
    TO_CHAR(first_stage_reversal_date_time__c,'DD-MON-YYYY'),
    last_from_to_stage__c,
    TO_CHAR(last_stage_reversal_date_time__c,'DD-MON-YYYY'),
    num_of_stage_reversal__c,
    q4_revenue_ready__c,
    lead_num__c,
    is_copy_quote__c,
    ocn__c,
    account_city__c,
    account_country__c,
    account_state__c,
    product_line__c,
    rmu__c,
    reporting_hierachy__c,
    challenger_learning_and_results__c,
    challenger_skills_applied__c,
	current_date,
    '%s',
    '%s'
FROM
    opportunity'''

'''INSER TABLE OPPORTUNITYSPLIT_SNAPSHOT'''

STMNT5=''' INSERT INTO OPPORTUNITYSPLIT_SNAPSHOT(
ID	,
ISDELETED	,
SPLIT	,
CREATEDDATE	,
CREATEDBYID	,
LASTMODIFIEDDATE	,
LASTMODIFIEDBYID	,
SYSTEMMODSTAMP	,
OPPORTUNITYID	,
SPLITOWNERID	,
SPLITPERCENTAGE	,
SPLITNOTE	,
SPLITTYPEID	,
SPLITAMOUNT	,
BOOKEDAMOUNT_SPLIT__C	,
BOOKEDAMOUNT_SPLIT_USD__C	,
CALCULATESPLITFECODE__C	,
--FECQUOTA__C	,
OPPORTUNITY__C	,
SPLIT_FE_CODE__C	,
TEAM_MEMBER__C	,
REPORTING_HIERARCHY__C	,
PERIOD_NAME	,
WORKDAY_INFO	,
SNAPSHOT_DT	
)

SELECT Id,
  IsDeleted,
  Split,
  CreatedDate,
  CreatedById,
  LastModifiedDate,
  LastModifiedById,
  SystemModstamp,
  OpportunityId,
  SplitOwnerId,
  SplitPercentage,
  SplitNote,
  SplitTypeId,
  SplitAmount,
  BookedAmount_Split__c,
  Booked_Order_Amount_Split_USD__c,
  CalculateSplitFECode__c,
  --FECQUOTA__c,
  Opportunity__c,
  Split_FE_Code__c,
  Team_member__c,
  Reporting_Hierarchy__c,
  '%s',
  '%s',
  current_date

FROM OpportunitySplit '''

LOADLIST=[{
  "Load_Seq": "1",
  "Load_Table":"ALL_OPPORTUNITIES",
  "Load_String": STMNT1,
  "Load_Frequency": "DAILY",
  "Is_Truncate":"YES"

  },
{
  "Load_Seq": "2",
  "Load_Table":"OPPORTUNITY_SNAPSHOT",
  "Load_String": STMNT2,
  "Load_Frequency": "WDONLY",
  "Is_Truncate":"NO"
  },
{
  "Load_Seq": "3",
  "Load_Table":"OPPORTUNITY_SNAPSHOT_DAILY",
  "Load_String": STMNT3,
  "Load_Frequency": "DAILY",
  "Is_Truncate":"NO"
  },
{
  "Load_Seq": "4",
  "Load_Table":"OPPTY_SNAPSHOT_ASIS",
  "Load_String": STMNT4,
  "Load_Frequency": "DAILY",
  "Is_Truncate":"NO"
  },

{
  "Load_Seq": "5",
  "Load_Table":"OPPORTUNITYSPLIT_SNAPSHOT",
  "Load_String": STMNT5,
  "Load_Frequency": "DAILY",
  "Is_Truncate":"NO"
  }


]

FUNNEL_IN_OUT_VAR='''    SELECT PERIOD_NAME,
      CURRENT_WD_INFO,
      CURRENT_SNAPSHOT_DATE,
      PREV_WD_INFO,
      PREV_SNAPSHOT_DATE,
      COMPARISON_NAME
     FROM(


SELECT MAX(PERIOD_NAME) AS PERIOD_NAME,
       CASE WHEN LENGTH(MAX(to_number(SUBSTR(WORKDAY_INFO,3,5))))>1 THEN 'WD'||MAX(to_number(SUBSTR(WORKDAY_INFO,3,5))) ELSE 'WD0'||MAX(to_number(SUBSTR(WORKDAY_INFO,3,5)))  END AS CURRENT_WD_INFO,
        MAX(SNAPSHOT_DT)                           AS CURRENT_SNAPSHOT_DATE,
        CASE WHEN LENGTH(MIN(to_number(SUBSTR(WORKDAY_INFO,3,5))))>1 THEN 'WD'||MIN(to_number(SUBSTR(WORKDAY_INFO,3,5))) ELSE 'WD0'||MIN(to_number(SUBSTR(WORKDAY_INFO,3,5)))  END  AS PREV_WD_INFO ,
        MIN(SNAPSHOT_DT)                           AS PREV_SNAPSHOT_DATE,
      MAX(TO_CHAR( SNAPSHOT_DT,'YYYY'))
        ||'-'
       ||
        MAX(UPPER(substr(TO_CHAR(SNAPSHOT_DT,'Month'),1,3)))
        ||'-'
        ||CASE WHEN LENGTH(MAX(to_number(SUBSTR(WORKDAY_INFO,3,5))))>1 THEN 'WD'||MAX(to_number(SUBSTR(WORKDAY_INFO,3,5))) ELSE 'WD0'||MAX(to_number(SUBSTR(WORKDAY_INFO,3,5)))END
        ||'-'
        ||CASE WHEN LENGTH(MIN(to_number(SUBSTR(WORKDAY_INFO,3,5))))>1 THEN 'WD'||MIN(to_number(SUBSTR(WORKDAY_INFO,3,5))) ELSE 'WD0'||MIN(to_number(SUBSTR(WORKDAY_INFO,3,5)))  END  AS COMPARISON_NAME
      FROM
        ( SELECT DISTINCT a.period_name,
          a.WORKDAY_INFO,
          DATE_TRUNC('DAY',a.SNAPSHOT_DT) AS SNAPSHOT_DT
        FROM opportunity_snapshot a
        WHERE TO_CHAR(a.SNAPSHOT_DT,'YYYY-MM') = 
        TO_CHAR(CURRENT_DATE,'YYYY-MM')
       -- '2018-03'
        
       -- SNAPSHOT_MONTH_IN
       -- AND a.WORKDAY_INFO                    IN (PREV_SNAPSHOT_WD_IN,CURR_SNAPSHOT_WD_IN)
        ORDER BY to_number(SUBSTR(WORKDAY_INFO,3,5)) DESC
        )
       LIMIT 2
       )
       
 WHERE CURRENT_WD_INFO<>PREV_WD_INFO;'''

CHECK_FUNNEL_COMP_EXISTS='''      SELECT COMPARISON_NAME
      
      FROM FUNNEL_IN_OUT
      WHERE COMPARISON_NAME='%s'
      LIMIT 1;'''


FUNNEL_IN_OUT_INSERT=''' 
INSERT INTO FUNNEL_IN_OUT(CATEGORY,
SUB_CATEGORY,
OPPTY_ID,
OPPTY_NUMBER,
CLOSE_DATE,
CURRENT_SPLIT_PERCENT,
PREVIOUS_SPLIT_PERCENT,
CURRENT_SALES_STAGE,
PREVIOUS_SALES_STAGE,
CURRENT_SPLIT_AMOUNT,
PREV_SPLIT_AMOUNT,
CURRENT_PERIOD_NAME,
PREVIOUS_PERIOD_NAME,
SNAPSHOT_DT,
WORKDAY_INFO_CURRENT,
WORKDAY_INFO_PREVIOUS,
SPLIT_FE_CODE,
CURRENT_CLOSE_MONTH,
PREVIOUS_CLOSE_MONTH,
PREVIOUS_SPLIT_ID,
CURRENT_SPLIT_ID,
PREVIOUS_OPPTY_NUMBER,
CURRENT_OPPTY_NUMBER,
PREVIOUS_SPLIT_FE,
CURRENT_SPLIT_FE,
COMPARISON_NAME,
OPPTY_CREATED_DATE,
PREVIOUS_SPLIT_HIER,
RECENT_SPLIT_HIER,
OPPTY_SPLIT_HIER,
ADJ_AMOUNT)



SELECT CASE WHEN SUB_CATEGORY = 'Intake' THEN 'Intake' WHEN SUB_CATEGORY  IN ( 'Booked','Lost') THEN 'Booked/Lost'
WHEN SUB_CATEGORY IN ( 'Push In','Pull In') THEN 'Moved In'
WHEN SUB_CATEGORY IN ( 'Push Out','Pull Out') THEN 'Moved Out' 
WHEN SUB_CATEGORY IN ( 'Split Revenue (+)','Split Revenue (-)') THEN 'Split Revenue Change'
WHEN SUB_CATEGORY IN ('Transfer Out' ,'Transfer In') THEN 'Transfer' 
WHEN SUB_CATEGORY IN ('No Change') THEN 'No Change' 
WHEN SUB_CATEGORY IN ('StartTotal') THEN 'StartTotal' 
ELSE 'Others'
END 
AS CATEGORY,
SUB_CATEGORY,
OPPTY_ID,
NVL(CURRENT_OPPTY_NUMBER ,PREVIOUS_OPPTY_NUMBER )AS OPPTY_NUMBER,
CLOSE_DATE,
CURRENT_SPLIT_PERCENT,
PREVIOUS_SPLIT_PERCENT,
CURRENT_SALES_STAGE,
PREVIOUS_SALES_STAGE,
CURRENT_SPLIT_AMOUNT,
PREV_SPLIT_AMOUNT,
NVL(CURRENT_PERIOD_NAME,PERIOD_NAME_VAR)AS CURRENT_PERIOD_NAME,
NVL(PREVIOUS_PERIOD_NAME,PERIOD_NAME_VAR)AS PREVIOUS_PERIOD_NAME,
TO_DATE('CURRENT_SNAPSHOT_DATE_VAR','MM/DD/YYYY') as SNAPSHOT_DT,
NVL(WORKDAY_INFO_CURRENT,CURRENT_WD_INFO_VAR) AS WORKDAY_INFO_CURRENT,
NVL(WORKDAY_INFO_PREVIOUS,PREV_WD_INFO_VAR) AS WORKDAY_INFO_PREVIOUS,
SPLIT_FE_CODE,
CURRENT_CLOSE_MONTH,
PREVIOUS_CLOSE_MONTH,
PREVIOUS_SPLIT_ID,
CURRENT_SPLIT_ID,
PREVIOUS_OPPTY_NUMBER,
CURRENT_OPPTY_NUMBER,
PREVIOUS_SPLIT_FE,
CURRENT_SPLIT_FE,
COMPARISON_NAME_VAR,
OPPTY_CREATED_DATE,
PREVIOUS_SPLIT_HIER,
RECENT_SPLIT_HIER,
OPPTY_SPLIT_HIER,
CASE WHEN SUB_CATEGORY = 'Intake' THEN CURRENT_SPLIT_AMOUNT WHEN SUB_CATEGORY  IN ( 'Booked','Lost') AND PREVIOUS_SPLIT_ID IS NULL
THEN 0 WHEN SUB_CATEGORY  IN ( 'Booked','Lost') THEN -PREV_SPLIT_AMOUNT WHEN SUB_CATEGORY IN ( 'Push In','Pull In') THEN CURRENT_SPLIT_AMOUNT
WHEN SUB_CATEGORY IN ( 'Push Out','Pull Out') THEN -PREV_SPLIT_AMOUNT 
WHEN SUB_CATEGORY IN ( 'Split Revenue (+)','Split Revenue (-)','Transfer In') THEN (CURRENT_SPLIT_AMOUNT - NVL(PREV_SPLIT_AMOUNT,0))
WHEN SUB_CATEGORY IN ('Transfer Out' ) THEN (- PREV_SPLIT_AMOUNT) 
WHEN SUB_CATEGORY = 'StartTotal' THEN StartTotalAmt
ELSE (CURRENT_SPLIT_AMOUNT - PREV_SPLIT_AMOUNT) END as ADJ_AMOUNT
 FROM 
(


SELECT 


  (
  CASE
    WHEN prev_snapshot.OPPTY_ID          IS NULL
    AND recent_snapshot.SALES_STAGE NOT  IN ('Booked','Lost')
    AND recent_snapshot.CREATED_DATE> TO_DATE('PREV_SNAPSHOT_DATE_VAR','MM/DD/YYYY')

  
    THEN 'Intake'
	
    WHEN (recent_snapshot.SPLIT_HIER_ID                      = prev_snapshot.SPLIT_HIER_ID
    OR prev_snapshot.SPLIT_HIER_ID                          IS NULL)
    AND (recent_snapshot.SALES_STAGE                         = 'Booked')
    AND ( 
	--TO_CHAR(recent_snapshot.CREATED_DATE,'YYYY-MM') = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
   -- OR 
	recent_snapshot.SALES_STAGE   <> nvl(prev_snapshot.SALES_STAGE,'aaa' )  )
 
    THEN 'Booked'
    WHEN (recent_snapshot.SPLIT_HIER_ID                      = prev_snapshot.SPLIT_HIER_ID
     OR prev_snapshot.SPLIT_FE_CODE                          IS NULL)
    AND (recent_snapshot.SALES_STAGE                         = 'Lost')
    AND (
	--TO_CHAR(recent_snapshot.CREATED_DATE,'YYYY-MM') = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
    --OR 
	recent_snapshot.SALES_STAGE   <> nvl(prev_snapshot.SALES_STAGE,'aaa'))
    THEN 'Lost'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID,'0')                                       = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    AND recent_snapshot.SALES_STAGE NOT                                                     IN ('Booked','Lost')
    AND (prev_snapshot.CLOSE_MONTH                                                           < TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
    AND (recent_snapshot.CLOSE_MONTH                                                        = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')))
   -- AND ((recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) = 0 )
    THEN 'Push In'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID,'0')                                                     = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    
    AND recent_snapshot.SALES_STAGE NOT                                                    IN ('Booked','Lost')
    AND (prev_snapshot.CLOSE_MONTH                                                          > TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
    AND (recent_snapshot.CLOSE_MONTH                                                        = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
  --  AND (recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) = 0
    THEN 'Pull In'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID,'0')                                              = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    AND (recent_snapshot.SALES_STAGE NOT                                                    IN ('Booked','Lost'))
    AND (prev_snapshot.CLOSE_MONTH                                                           = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
    AND (recent_snapshot.CLOSE_MONTH                                                         > TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
    AND (prev_snapshot.CLOSE_MONTH                                                          <> recent_snapshot.CLOSE_MONTH)
   -- AND ((recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) =0 )
    THEN 'Push Out'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID ,'0')                                           = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    AND recent_snapshot.SALES_STAGE NOT                                                     IN ('Booked','Lost')
    AND (prev_snapshot.CLOSE_MONTH                                                           = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
    AND (recent_snapshot.CLOSE_MONTH                                                         < TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
  --  AND ((recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) = 0
  --  OR (recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT   - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) = 0 )
    THEN 'Pull Out'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID,'0')                                                     = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    AND (prev_snapshot.CLOSE_MONTH                                                          = recent_snapshot.CLOSE_MONTH)
    AND (recent_snapshot.SALES_STAGE NOT                                                   IN ('Booked','Lost'))
    AND (recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) > 0
    THEN 'Split Revenue (+)'
    WHEN (nvl(recent_snapshot.SPLIT_HIER_ID,'0')                                         = nvl(prev_snapshot.SPLIT_HIER_ID,'0'))
    AND (prev_snapshot.CLOSE_MONTH                                                          = recent_snapshot.CLOSE_MONTH)
    AND (recent_snapshot.SALES_STAGE NOT                                                   IN ('Booked','Lost'))
    AND (recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT - prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT) < 0
    THEN 'Split Revenue (-)'


    WHEN  recent_snapshot.OPPTY_ID IS NULL AND check_transfer_out.OPPTY_ID IS NOT NULL
    AND (prev_snapshot.SPLIT_HIER_ID <> nvl(recent_snapshot.SPLIT_HIER_ID,'0')  )
    AND (prev_snapshot.CLOSE_MONTH  = TO_CHAR(TO_DATE('CURRENT_SNAPSHOT_DATE_VAR','MM/DD/YYYY'),'YYYY-MM') )
    
    
    
    AND (prev_snapshot.SALES_STAGE NOT                                                   IN ('Booked','Lost'))
   
    THEN 'Transfer Out'
    
    WHEN  prev_snapshot.OPPTY_ID IS NULL AND  check_transfer_in.OPPTY_ID IS NOT NULL
    AND  ( prev_snapshot.SPLIT_HIER_ID <> recent_snapshot.SPLIT_HIER_ID OR  prev_snapshot.SPLIT_HIER_ID IS NULL)
    AND (recent_snapshot.CLOSE_MONTH  = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM'))
    AND (recent_snapshot.SALES_STAGE NOT                                                   IN ('Booked','Lost'))
   
    THEN 'Transfer In'
    ELSE 'No Change'
  END) AS SUB_CATEGORY,
  0 as StartTotalAmt,
   NVL(recent_snapshot.OPPTY_ID,prev_snapshot.OPPTY_ID) AS OPPTY_ID,
  NVL(recent_snapshot.CLOSE_DATE,prev_snapshot.CLOSE_DATE) AS CLOSE_DATE,
  recent_snapshot.SALES_STAGE CURRENT_SALES_STAGE,
  prev_snapshot.SALES_STAGE PREVIOUS_SALES_STAGE,
  recent_snapshot.SPLIT_PERCENT AS CURRENT_SPLIT_PERCENT,
  prev_snapshot.SPLIT_PERCENT AS PREVIOUS_SPLIT_PERCENT,
    recent_snapshot.OPPORTUNITY_SPLIT_AMOUNT CURRENT_SPLIT_AMOUNT,
  prev_snapshot.OPPORTUNITY_SPLIT_AMOUNT AS PREV_SPLIT_AMOUNT,
  recent_snapshot.PERIOD_NAME CURRENT_PERIOD_NAME,
  prev_snapshot.PERIOD_NAME PREVIOUS_PERIOD_NAME,
  recent_snapshot.SNAPSHOT_DT,
  recent_snapshot.WORKDAY_INFO AS WORKDAY_INFO_CURRENT,
  prev_snapshot.WORKDAY_INFO   AS WORKDAY_INFO_PREVIOUS,
  recent_snapshot.COMP_KEY_ID,
  recent_snapshot.CREATED_DATE,
  --TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM') as TEST,
  recent_snapshot.SPLIT_FE_CODE,
  recent_snapshot.CLOSE_MONTH AS CURRENT_CLOSE_MONTH,
  prev_snapshot.CLOSE_MONTH   AS PREVIOUS_CLOSE_MONTH,
  recent_snapshot.COMP_KEY_ID AS COMP_KEY_ID_WD5,
  prev_snapshot.COMP_KEY_ID   AS COMP_KEY_ID_WD3,
  prev_snapshot.SPLIT_ID PREVIOUS_SPLIT_ID,
  recent_snapshot.SPLIT_ID AS CURRENT_SPLIT_ID,
  prev_snapshot.OPPTY_NUMBER  as PREVIOUS_OPPTY_NUMBER,
 recent_snapshot.OPPTY_NUMBER as CURRENT_OPPTY_NUMBER,
prev_snapshot.SPLIT_FE_CODE as PREVIOUS_SPLIT_FE,
 recent_snapshot.SPLIT_FE_CODE as CURRENT_SPLIT_FE ,
 COMPARISON_NAME_VAR AS COMPARISON_NAME,
--( SELECT recent_snapshot.WORKDAY_INFO||'-'||prev_snapshot.WORKDAY_INFO|| )
NVL(recent_snapshot.CREATED_DATE,prev_snapshot.CREATED_DATE) AS OPPTY_CREATED_DATE,
prev_snapshot.SPLIT_HIER_ID as PREVIOUS_SPLIT_HIER,
recent_snapshot.SPLIT_HIER_ID as RECENT_SPLIT_HIER,
NVL(recent_snapshot.SPLIT_HIER_ID,prev_snapshot.SPLIT_HIER_ID) AS OPPTY_SPLIT_HIER

  
FROM
  (SELECT *
  FROM opportunity_snapshot
  WHERE period_name=PERIOD_NAME_VAR
  AND WORKDAY_INFO =CURRENT_WD_INFO_VAR
  AND SALES_STAGE NOT IN ('Closed','Open')
  ) recent_snapshot
  
  FULL OUTER JOIN
  (SELECT *
  FROM opportunity_snapshot
  WHERE period_name=PERIOD_NAME_VAR
  AND WORKDAY_INFO =PREV_WD_INFO_VAR
  AND SALES_STAGE NOT IN ('Closed','Open')
  ) prev_snapshot
ON recent_snapshot.OPPTY_ID||NVL (recent_snapshot.SPLIT_HIER_ID,'0') = prev_snapshot.OPPTY_ID||nvl(prev_snapshot.SPLIT_HIER_ID,'0')
  
  FULL OUTER JOIN 
(SELECT DISTINCT OPPTY_ID
  FROM opportunity_snapshot
  WHERE period_name=PERIOD_NAME_VAR
  AND WORKDAY_INFO =PREV_WD_INFO_VAR
  AND SALES_STAGE NOT IN ('Closed','Open')
 -- AND  OPPTY_NUMBER='118861'
  ) check_transfer_in
  
 
 ON  recent_snapshot.OPPTY_ID=check_transfer_in.OPPTY_ID
 
  FULL OUTER JOIN 
(SELECT DISTINCT OPPTY_ID
  FROM opportunity_snapshot
  WHERE period_name=PERIOD_NAME_VAR
  AND WORKDAY_INFO =CURRENT_WD_INFO_VAR
  AND SALES_STAGE NOT IN ('Closed','Open')
-- AND  OPPTY_NUMBER='118861'
  ) check_transfer_out
  

 ON  prev_snapshot.OPPTY_ID=check_transfer_out.OPPTY_ID
WHERE 
prev_snapshot.CLOSE_MONTH                                                           = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
OR recent_snapshot.CLOSE_MONTH = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
OR CASE WHEN recent_snapshot.CLOSE_MONTH IS NOT NULL AND prev_snapshot.CLOSE_MONTH IS NULL THEN recent_snapshot.CLOSE_MONTH   END
    
    = TO_CHAR(recent_snapshot.SNAPSHOT_DT,'YYYY-MM')
    
OR CASE WHEN prev_snapshot.CLOSE_MONTH IS NOT NULL AND recent_snapshot.CLOSE_MONTH IS NULL THEN prev_snapshot.CLOSE_MONTH   END
    
    =  TO_CHAR(TO_DATE('CURRENT_SNAPSHOT_DATE_VAR','MM/DD/YYYY'),'YYYY-MM'));
  
  

  '''
