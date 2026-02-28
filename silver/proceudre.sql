---PROCEDURE: silver.load_silver
-- PURPOSE: Load and transform data from Bronze layer tables 
-- (CRM & ERP systems) into Silver layer tables
-- USAGE: CALL silver.load_silver();
create or replace procedure silver.load_silver()
language plpgsql
as $$
begin 
--======================================================================-
		truncate silver.crm_cust_info; 
		-- Transformation script for crm_cust_info using CTE
		insert into silver.crm_cust_info (
		       cst_id,
		        cst_key,
		        cst_firstname,
		        cst_lastname,
		        cst_marital_status,
		        cst_gndr,
		        cst_create_date)
		with transformation as(
		select 
		        cst_id,
		        cst_key,
		        cst_firstname,
		        cst_lastname,
		        cst_marital_status,
		        cst_gndr,
		        cst_create_date,
		        ROW_number() over(partition by cst_id order by cst_create_date desc)as flag_last 
		from bronze.crm_cust_info cci 
		where cst_id is not null -- null values filttred early for DATA QUALITY 
		)
		select 
		      cst_id,
		      cst_key,
		      trim(cst_firstname)as cst_firstname,-- Trim whitespace--
		      trim(cst_lastname)as cst_lastname, ---- Trim whitespace--
		      case when upper(trim(cst_marital_status))= 'M' then 'Married'
		      when upper(trim(cst_marital_status))= 'S' then 'Single'
		      else 'N/A' end  as cst_marital_status,-- standardize categorical columns--
		      case when UPPER(TRIM(cst_gndr)) = 'M' then 'Male' 
		    when UPPER(TRIM(cst_gndr)) ='F' then 'Female' 
		    else  'N/A'end as cst_gndr,-- standardize categorical columns--
		    cst_create_date
		    from transformation 
		    where flag_last = 1 ;
		    
	
		---=====================transformation_crm_prd ==========================--
		truncate silver.crm_prd_info;
		drop table if exists silver.crm_prd_info;
		create table silver.crm_prd_info(
		            prd_id int,
		            cat_id varchar(20),
		            prd_key varchar(50),
		            prd_cost integer,
		            prd_line varchar(50),
		            prd_start_dt date,
		            prd_end_dt date );
		insert into silver.crm_prd_info (
		           prd_id,
		           cat_id,
		           prd_key,
		           prd_cost,
		           prd_line,
		           prd_start_dt,
		           prd_end_dt
		)
		with transformation_crm_prd as(
		     select 
		           prd_id ,
		           replace(substring(prd_key,1,5),'-','_')as cat_id, -- cat_id is derived from prd_key 
		                                                              -- which references to bronze.erp_px_car_g1v2 epcgv (id) 
		           substring(prd_key,7,length(prd_key)) as prd_key,          --- prd_key is references to crm_sales_details csd (sls_prd_key)                                       
		           coalesce(cpi.prd_cost ,0 )as prd_cost,
		           case upper(trim(cpi.prd_line ))
		             	when 'M' then 'Mountain' 
		            	when 'R' then 'Road'
		            	when 'S' then 'other sales'
		                when 'T' then 'Touring'
		                else 'N/A'
		           end as prd_line ,
		           prd_start_dt,
		           --  I set prd_end_dt = next start_date - 1 day
		           -- so  the end is always before the next row's start_date
		           lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )- 1 as prd_end_dt
		from bronze.crm_prd_info cpi
		)
		select 
		     prd_id,
		     cat_id,
		     prd_key,
		     prd_cost,
		     prd_line,
		     prd_start_dt,
		     prd_end_dt
		from  transformation_crm_prd;
		---===============================================--
		--check it 
		
		select *
		from silver.crm_prd_info cpi 
		where cpi.prd_cost < 0 or cpi.prd_cost  is null; -- fine
		
		select cpi.prd_id ,
		       count(*)
		from silver.crm_prd_info cpi 
		group by  cpi.prd_id 
		having count(*) >1 ;-- fine 
		
		select cpi.prd_line
		from silver.crm_prd_info cpi  
		where  cpi.prd_line != trim(cpi.prd_line );  --fine
		
		select distinct cpi.prd_line
		from silver.crm_prd_info cpi  ;  --fine
		
		select *
		from silver.crm_prd_info cpi 
		where cpi.prd_end_dt < cpi.prd_start_dt  ; --fine
		
		select * 
		from silver.crm_prd_info cpi 
		
		--======================transform crm_sales_details============================--
		truncate silver.crm_sales_details ;
		drop table if exists silver.crm_sales_details ;
		create table if not exists silver.crm_sales_details(
		      sls_ord_num varchar(50) ,
		      sls_prd_key varchar(50) ,
		      sls_cust_id integer ,
		      sls_order_dt date,
		      sls_ship_dt date ,
		      sls_due_dt date ,
		      sls_sales integer ,
		      sls_quantity int,
		      sls_price integer
		);
		insert into silver.crm_sales_details(
		      sls_ord_num ,
		      sls_prd_key ,
		      sls_cust_id ,
		      sls_order_dt ,
		      sls_ship_dt ,
		      sls_due_dt ,
		      sls_sales ,
		      sls_quantity ,
		      sls_price )
		with transformation_crm_sales_details as(
		     select 
		           csd.sls_ord_num,
		           csd.sls_prd_key ,
		           csd.sls_cust_id,
		           case 
		              when csd.sls_order_dt = 0 or length(csd.sls_order_dt::text)!=8 then null
		              else cast(cast(csd.sls_order_dt as varchar)as date)
		           end as sls_order_dt,
		           case 
		               when csd.sls_ship_dt = 0 or length(csd.sls_ship_dt::text)!=8 then null
		               else cast(cast(csd.sls_ship_dt as varchar)as date)
		           end as sls_ship_dt,
		           case 
		               when csd.sls_due_dt = 0 or length(csd.sls_due_dt::text)!=8 then null
		           	   else cast(cast(csd.sls_due_dt as varchar)as date)
		           end as sls_due_dt,
		           case 
		           	   when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		           	   then  sls_quantity * ABS(sls_price)
		           	   else  sls_sales
		           end as sls_sales,
		           sls_quantity,
		           case 
		           	  when sls_price is null or sls_price <= 0 
		           	  then sls_price / NULLIF(sls_quantity,0)
		           	  else abs(sls_price)
		           end as sls_price
		    from bronze.crm_sales_details csd 
		)
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
		from silver.crm_sales_details csd ;
		
		
		select *
		from silver.crm_sales_details csd;
		---==========================================---
		
		--===================transformation erp_system_tables==================---
		truncate  silver.erp_cust;
		insert into silver.erp_cust(
		           cid ,
		           bdate,
		           gen
		 )
		with transformation_erp_cut as(
		    select 
		         case 
		         	 when cid like 'NAS%' then substring(cid,4,length(cid))
		         	 else cid
		         end as cid ,
		         case 
		         when bdate > CURRENT_DATE  then null 
		         else bdate
		        end as bdate,
		        case
		        	when upper(trim(gen)) in ('F','FEMALE') then 'Female'
		        	when upper(trim(gen)) in ('M','MALE') then 'Male' 
		        	else 'N/A'
		        end as gen
		 from bronze.erp_cust ec 
		)
		select 
		           cid ,
		           bdate,
		           gen
		from transformation_erp_cut;
		
		--====================================--
		
		--===============trnsformation_ bronze.erp_loc el-=================--
		truncate SILVER.erp_loc;
		insert into SILVER.erp_loc (cid,cntry)
		with transformation_erp_loc  as (
		    select
		          replace(cid ,'-','')as cid ,
		          case 
		          	when trim(cntry) ='DE' then 'Germany'
		          	when trim(cntry) in ('US','USA') then 'United States'
		          	when trim(cntry)= '' or cntry is null then 'N/A'
		          	else trim(cntry)
		          end as cntry 
		from bronze.erp_loc el )
		select cid ,
		       cntry 
		from transformation_erp_loc;
		--check it 
		select *
		from silver.erp_loc el 
		
		--=================transformation_erp_px_car_g1v2===================--
		truncate silver.erp_px_car_g1v2 ;
		insert into silver.erp_px_car_g1v2 (  
		           id,
		           cat,
		           subcat ,
		           maintenance )
		with transformation_erp_px_car_g1v2 as (
		     select 
		           id,
		           cat,
		           subcat ,
		           maintenance 
		from bronze.erp_px_car_g1v2 
		)
		select    
		           id,
		           cat,
		           subcat ,
		           maintenance 
		from bronze.erp_px_car_g1v2 ;
		
		--- check it 
		select *
		from silver.erp_px_car_g1v2 epcgv ;
		
end ;
$$;
