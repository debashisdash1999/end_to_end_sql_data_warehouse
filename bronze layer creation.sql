USE Datawarehouse_Project;
-- DDL Statements for CRM files
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- DDL Statements for ERP files
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);


-- Inserting each CSV file to each table by BULK INSERT
/*
-- bronze.crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.crm_cust_info;

-- bronze.crm_prd_info
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.crm_prd_info

-- bronze.crm_sales_details
TRUNCATE TABLE bronze.crm_sales_details
BULK INSERT bronze.crm_sales_details
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.crm_sales_details;

-- bronze.erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101
BULK INSERT bronze.erp_loc_a101
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.erp_loc_a101;

-- bronze.erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12
BULK INSERT bronze.erp_cust_az12
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.erp_cust_az12;

-- bronze.erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	  FIRSTROW = 2,
	  FIELDTERMINATOR = ',',
	  TABLOCK
	 );
SELECT * FROM bronze.erp_px_cat_g1v2;
*/

/* Since this script needs to run daily to process the incoming data, I will store it inside a stored procedure. 
   The stored procedure will run daily here.

-- CREATE STORED PROCEDURE WITH UPPER INSERT SCRIPTS FOR INSERTING DAILY DATA
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declaring variables to keep an eye on the loading times of each tables
	DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY

	    -- Messgae printing for better understanding
	    PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- bronze.crm_cust_info
		--Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		-- bronze.crm_prd_info
		--Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- bronze.crm_sales_details
	    --Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- bronze.erp_loc_a101
		--Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- bronze.erp_cust_az12
		--Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

        -- bronze.erp_px_cat_g1v2
		--Using the VARIABLE here to see the start time
		SET @start_time = GETDATE();
		PRINT '>> INSERT into bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
		--Using the VARIABLE here to see the end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
    END CATCH
END;
GO

-- Run this to insert new data daily
EXEC bronze.load_bronze;
*/

-- CREATE STORED PROCEDURE WITH INSERT SCRIPTS FOR INSERTING DAILY DATA (Properly formatted and commented version)
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declare variables to track load duration for each table
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY

        -- High-level progress messages for readability in Messages tab
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        /* 
           Using GETDATE() to capture the start time for this table load.
           These timestamps are later used with DATEDIFF to calculate duration in seconds. [web:11]
        */
        -- bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.crm_cust_info';

        -- TRUNCATE removes all rows quickly and resets the table, without logging each row delete. [web:12]
        TRUNCATE TABLE bronze.crm_cust_info;

        -- BULK INSERT loads data directly from a CSV file into the table using minimal logging for speed. [web:12]
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,          -- Skip header row in the CSV
            FIELDTERMINATOR = ',', -- Comma-separated values
            TABLOCK                -- Take a table lock to optimize bulk load
        );

        -- Capture end time and print duration using DATEDIFF in seconds. [web:11]
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.crm_prd_info';

        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- bronze.crm_sales_details
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.crm_sales_details';

        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.erp_loc_a101';

        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.erp_cust_az12';

        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.erp_px_cat_g1v2';

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

    END TRY
    BEGIN CATCH
        /* 
           ERROR_MESSAGE, ERROR_NUMBER and ERROR_STATE return details about the error
           that triggered the CATCH block, useful for debugging and logging. [web:3][web:20][web:22]
        */
        PRINT '==========================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO

-- Run this to insert new data daily
EXEC bronze.load_bronze;



