/* Load Data into Bronze Layer

-All raw CSV datasets (CRM, ERP) were imported into the Bronze Layer tables.  

- Customer data → `bronze.crm_cust_info`  
- Product data → `bronze.crm_prd_info`  
- Sales data → `bronze.crm_sales_details`  
- ERP Customer → `bronze.erp_CUST`  
- ERP Location → `bronze.erp_LOC`  
- ERP Car Data → `bronze.erp_PX_CAR_G1V2`  

The data was loaded using **DBeaver Import Wizard** for local CSV files */

--check the load  

--crm_cust_info

    select count(*)
    from bronze.crm_cust_info cci ;

-- crm_prd_info
    
    select count(*)
    from bronze.crm_prd_info cpi ;

--- crm_sales_details
    
    select count(*)
    from bronze.crm_sales_details csd ;

--- erp_cust
    
    select count(*)
    from bronze.erp_cust ec ;

    --erp_loc
    
     select count(*)
     from bronze.erp_loc el ;

     --erp_px_car_g1v2
     
     select count(*)
     from bronze.erp_px_car_g1v2 epcgv ;

    
