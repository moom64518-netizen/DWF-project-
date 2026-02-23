/*this script stored procedure `backup_bronze_layer_data` that copies all data from 
the `bronze` schema tables into the `load_bronze_layer_data` schema for backup.

## Tables included

- crm_cust_info
- crm_prd_info
- crm_sales_details
- erp_cust
- erp_loc
- erp_px_car_g1v2`*/


-- create procedure to backup current Bronze Layer data

CREATE OR REPLACE PROCEDURE backup_bronze_layer_data()
LANGUAGE plpgsql
AS $$
DECLARE 
     start_time timestamp;
     end_time timestamp;
     duration interval;
BEGIN
    
     BEGIN --TRY  CHECK ERRORS
     raise notice 'upload crm tables';


   -- ================= crm_cust_info =================
       start_time:=clock_timestamp();

    INSERT INTO load_bronze_layer_data.crm_cust_info
    SELECT * FROM bronze.crm_cust_info cci;
    
       end_time:=clock_timestamp();
       duration:= end_time - start_time ;

      raise notice 'crm_cust_info took:% ms',extract(EPOCH FROM duration)*1000;
                

    -- ================= crm_prd_info =================

        start_time:=clock_timestamp();

    INSERT INTO load_bronze_layer_data.crm_prd_info
    SELECT * FROM bronze.crm_prd_info cpi;
        end_time:=clock_timestamp();
        duration:= end_time - start_time;
     raise notice 'crm_prd_info took:% ms',extract(EPOCH FROM duration)*1000;
    
      -- ================= crm_sales_details =================

        start_time:=clock_timestamp();

    INSERT INTO load_bronze_layer_data.crm_sales_details
    SELECT * FROM bronze.crm_sales_details csd;
       end_time:=clock_timestamp();
       duration:= end_time - start_time ;
      raise notice 'crm_sales_details took:% ms',extract(EPOCH FROM duration)*1000;
   
    raise notice 'upload erp tables';

      -- ================= erp_cust =================
         start_time:=clock_timestamp();

    INSERT INTO load_bronze_layer_data.erp_cust
    SELECT * FROM bronze.erp_cust ec;
         end_time:=clock_timestamp();
         duration:= end_time - start_time ;
      raise notice 'erp_cust took:% ms',extract(EPOCH FROM duration)*1000;

       -- ================= erp_loc =================

         start_time:=clock_timestamp();
    INSERT INTO load_bronze_layer_data.erp_loc
    SELECT * FROM bronze.erp_loc el;
         end_time:=clock_timestamp();
         duration:= end_time - start_time ;
          raise notice 'erp_loc took:% ms',extract(EPOCH FROM duration)*1000;

    -- ================= erp_px_car_g1v2 =================

           start_time:=clock_timestamp();

    INSERT INTO load_bronze_layer_data.erp_px_car_g1v2
    SELECT * FROM bronze.erp_px_car_g1v2 epcgv;
           end_time:=clock_timestamp();
           duration:= end_time - start_time ;
    raise notice 'erp_px_car_g1v2 took:% ms',extract(EPOCH FROM duration)*1000;
    RAISE NOTICE 'ALL BRONZE DATA BACKED UP SUCCESSFULLY';
    EXCEPTION
    WHEN OTHERS THEN  
                    RAISE NOTICE 'error occourred%',SQLERRM;
    END;

END;
$$;
 
--check IT
SELECT routine_schema, routine_name, routine_type
FROM information_schema.routines
WHERE routine_name = 'backup_bronze_layer_data';

call public.backup_bronze_layer_data()--done 
