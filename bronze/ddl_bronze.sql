
/*DDL_SCRIPT :CREATE BRONZE TABLES
this script creates a tables in the 'bronze' schema*/

--================ddl_script_crm ===============
  --====create_crm_cust_info table====--
create table if not exists bronze.crm_cust_info (
  cst_id int,
  cst_key varchar,
  cst_firstname varchar(50),
  cst_lastname varchar(50),
  cst_marital_status varchar(50),
  cst_gndr varchar(50),
  cst_create_date date
);

 --====create crm_prd_info table ===--
create table if not exists bronze.crm_prd_info(
   prd_id int ,
   prd_key varchar ,
   prd_nm varchar(50),
   prd_cost varchar(50),
   prd_line varchar(50),
   prd_start_dt date ,
   prd_end_dt date 
);
--===create crm_sales_details table===--
create table if not exists bronze.crm_sales_details(
   sls_ord_num varchar(50),
   sls_prd_key varchar(50),
   sls_cust_id integer ,
   sls_order_dt date ,
   sls_ship_dt date ,
   sls_due_dt date ,
   sls_sales integer,
   sls_quantity int,
   sls_price numeric
);
--================ddl_script_erp ===============
--===create erp_CUST table===--
create table if not exists bronze.erp_CUST(
  CID VARCHAR(50),
  BDATE DATE ,
  GEN  VARCHAR
);
--===create erp_LOC table ===--
create table if not exists  bronze.erp_LOC(
  CID VARCHAR (50),
  CNTRY VARCHAR(50)
);
--===create erp_PX_CAR_G1V2 table ===--
create table if not exists  bronze.erp_PX_CAR_G1V2(
  ID VARCHAR,
  CAT VARCHAR(50),
  SUBCAT VARCHAR,
  MAINTENANCE boolean
);
