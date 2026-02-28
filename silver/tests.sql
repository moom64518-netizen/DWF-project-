
-- Data Quality & Consistency Checks for Silver Layer Tables
-- 
-- This script verifies the transformations and loads by checking:
-- 1. Duplicate IDs in crm_cust_info
-- 2. Unwanted leading/trailing spaces in text columns
-- 3. Consistency of categorical columns (gender, marital_status, product lines)
-- 4. Validity of dates (order, ship, due, birthdates)
-- 5. Referential integrity between CRM and ERP tables
-- 6. Data standardization in ERP location and product tables

--check the  tranformation script --
		    --- check out IDs dublicates --
		    select 
		       cci.cst_id ,
		       count(*)
		    from silver.crm_cust_info cci
		    group by cci.cst_id 
		    having  count(*) > 1 -- GREAT
		    
		  -- FIND OUT unwanted spaces
		select 
		     cci.cst_lastname 
		from silver.crm_cust_info cci
		where  
		  cci.cst_lastname != trim(cci.cst_lastname); --GREAT---
		select 
		     cci.cst_firstname 
		from silver.crm_cust_info cci
		where  
		  cci.cst_firstname  != trim(cci.cst_firstname ); --GREAT---
		--check the cosistency 
		  select  distinct CCI.
		  cst_marital_status  
		  from silver.crm_cust_info cci ;
		--check the cosistency 
		select distinct cci.cst_gndr 
		from silver.crm_cust_info cci ; --DONE--
		  ---===========================================================================--
		
		

--===================check crm_sls_detils=======================--


select 
      sls_ord_num ,
      sls_prd_key ,
      sls_cust_id ,
      sls_order_dt ,
      sls_ship_dt ,
      sls_due_dt ,
      sls_sales ,
      sls_quantity ,
      sls_price 
from silver.crm_sales_details csd 
where csd.sls_ord_num != trim(csd.sls_ord_num); -- fine no unwanted spaces

---check the match columns 
select 
      csd.sls_ord_num ,
      csd.sls_prd_key ,
      csd.sls_cust_id ,
      csd.sls_order_dt ,
      csd.sls_ship_dt ,
      csd.sls_due_dt ,
      csd.sls_sales ,
      csd.sls_quantity ,
      csd.sls_price 
from silver.crm_sales_details csd 
where  csd.sls_cust_id not in (
 select cci.cst_id 
 from silver.crm_cust_info cci ) ;---no outbut there's no unmatch rows
 
 
 
 --- check for valid date 
 select csd.sls_due_dt  
 from silver.crm_sales_details csd 
 where csd.sls_due_dt <=0  or
 length(csd.sls_due_dt::text)!= 8 or 
 sls_due_dt >= 20501010 or 
 sls_due_dt <  19000102  --no issue
 -- ther's a issue  so we will 
 --replace all zero values to null before we cast the column to data
 
 select csd.sls_sales ,
       csd.sls_price ,
       csd.sls_quantity 
from silver.crm_sales_details csd 
where csd.sls_sales is null or csd.sls_sales!= sls_quantity *abs(sls_price) 
or csd.sls_quantity  is null or sls_quantity =0 ;
--- invalid date 
select 
      csd.sls_order_dt ,
      csd.sls_ship_dt ,
      csd.sls_due_dt 
from silver.crm_sales_details csd 
where sls_order_dt > sls_ship_dt  
-- 

---====check erpp_system_tsbles 
--date 
select ec.bdate 
from bronze.erp_cust ec
where ec.bdate < '1924-01-01' or ec.bdate > CURRENT_DATE;
--- check diff ides
select ec.cid ,
 case 
         	 when cid like 'NAS%' then substring(cid,4,length(cid))
         	 else cid
         end as cid
from bronze.erp_cust ec
where case 
         	 when cid like 'NAS%' then substring(cid,4,length(cid))
         	 else cid
         end  not in (select cci.cst_key  from silver.crm_cust_info cci );
select distinct ec.gen 
from bronze.erp_cust ec ;
select * 
from bronze.erp_cust ec;

---check the keys 
--
select replace(el.cid,'-','') as cid 
from bronze.erp_loc el
where replace(el.cid,'-','') not in(select cci.cst_key 
from silver.crm_cust_info cci) ;


---data standraitation and consincty 
select distinct  el.cntry as old_C,
                 case 
          	when trim(cntry) ='DE' then 'Germany'
          	when trim(cntry) in ('US','USA') then 'United States'
          	when trim(cntry)= '' or cntry is null then 'N/A'
          	else trim(cntry)
          end as cntry 
from  bronze.erp_loc el ;
--======== check ides
select id 
from bronze.erp_px_car_g1v2
where id not in (select cpi.cat_id 
from silver.crm_prd_info cpi );
----- un wanted spaces 

select subcat  
from bronze.erp_px_car_g1v2
where trim(subcat )!= subcat ; --fine
----- un wanted spaces 
select maintenance 
from bronze.erp_px_car_g1v2
where trim(maintenance) != maintenance ; --fine
-------data standaritation and data consistency
						select distinct cat 
						from bronze.erp_px_car_g1v2  ;--fine
						
						select distinct subcat  
						from bronze.erp_px_car_g1v2 ;-- fine ;
						
						select distinct maintenance  
						from bronze.erp_px_car_g1v2 ; ---fine 
						
						select *
						from silver.erp_px_car_g1v2 epcgv ;


						
