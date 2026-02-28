 -- DDL_SCRIPT : CREATE   'SILVER' TABLES 
-- Script purpose : This Script creates tables in  THE 'Silver' Schema  , Dropping existing tables if they alredy exists.
---DDL create silver layer tables 

--CRM SYSTEM 
create table if not exists  silver.crm_cust_info(
like bronze.crm_cust_info INCLUDING all
);

create table if not exists  silver.crm_prd_info (
like bronze.crm_prd_info including all);

create table if not exists  silver.crm_sales_details (
like bronze.crm_sales_details including all);

---ERP SYSTEM 
create table if not exists silver.erp_cust(
like bronze.erp_cust including all);

create table if not exists silver.erp_loc(
like bronze.erp_loc including all);

create table if not exists silver.erp_px_car_g1v2(
like bronze.erp_px_car_g1v2 including all);
