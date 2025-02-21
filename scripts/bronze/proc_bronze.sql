
CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME
	BEGIN TRY
		PRINT '=====================================';
		PRINT 'Loading BRONZE Layer';
		PRINT '=====================================';

		PRINT '=====================================';
		PRINT '		Loading CRM TABLEs';
		PRINT '=====================================';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);
		
		--SELECT * FROM bronze.crm_cust_info;
		--SELECT count(*) FROM bronze.crm_cust_info;


		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);

		--SELECT * FROM bronze.crm_prd_info;
		--SELECT count(*) FROM bronze.crm_prd_info;



		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);

		--SELECT * FROM bronze.crm_sales_details;
		--SELECT count(*) FROM bronze.crm_sales_details;
		PRINT '=====================================';
		PRINT '		Loading ERP TABLEs';
		PRINT '=====================================';

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);

		--SELECT * FROM bronze.erp_loc_a101;
		--SELECT count(*) FROM bronze.erp_loc_a101;


		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);

		--SELECT * FROM bronze.erp_cust_az12;
		--SELECT count(*) FROM bronze.erp_cust_az12;



		TRUNCATE TABLE  bronze.erp_px_cat_g1v2;
		BULK INSERT  bronze.erp_px_cat_g1v2
		FROM 'C:\Users\abhij\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 TABLOCK
		);


		--SELECT * FROM bronze.erp_px_cat_g1v2;
		--SELECT count(*) FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT ' ========================================';
		PRINT 'ERROR IN LOADING BRONZE LAYER';
		PRINT 'ERROR MSG' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'ERROR MSG' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT ' ========================================';
	END CATCH
END

-- Uncomment below command to run procedure
--- EXEC bronze.load_bronze
