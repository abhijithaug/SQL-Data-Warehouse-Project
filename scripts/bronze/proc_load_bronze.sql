/*
===============================
STORED PROCEDURE: A stored procedure is a group of SQL statements that are created and stored in a database management system, 
allowing multiple users and programs to share and reuse the procedure. A stored procedure can accept input parameters, 
perform the defined operations, and return multiple output values.
========================================================
/*

--EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time = GETDATE();
			PRINT '=========================================';
			PRINT ' Loading Bronze Layer'
			PRINT '=========================================';

			PRINT ' ----------------------------------------';
			PRINT ' Loading CRM Tables';
			PRINT ' ----------------------------------------';

		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
			PRINT'>> INSERTING Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'


		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
			PRINT'>> INSERTING Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'


		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
			PRINT'>> INSERTING Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'



			PRINT ' ----------------------------------------';
			PRINT ' Loading ERP Tables';
			PRINT ' ----------------------------------------';

		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT'>> INSERTING Data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'


		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT'>> INSERTING Data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second,@start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'


		SET @start_time = GETDATE();
			PRINT'>> TRUNCATING Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			PRINT'>> INSERTING Data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH 
			(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'------------------------------------------------'

		SET @batch_end_time = GETDATE();
			PRINT'========================================'
			PRINT ' lOADING BRONZE LAYER IS COMPLETED'
			PRINT'========================================'
			PRINT '>> TOTAL Load Duration for BRONZE LAYER: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT'========================================'

		END TRY
		BEGIN CATCH
			PRINT '==========================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT '==========================================';
		END CATCH
	END 

--EXEC bronze.load_bronze





/*
============================
simplified code
=============================
/*

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);


TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);


TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);


TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);


TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);


TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM'C:\Users\abhijith\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH 
	(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
	);

END

--EXEC bronze.load_bronze
