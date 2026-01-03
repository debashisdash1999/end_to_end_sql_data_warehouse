/* ============================================================
   Database Context
   ------------------------------------------------------------
   Sets the active database for all subsequent DDL and DML
   operations in this script.
   ============================================================ */
USE Datawarehouse_Project;
GO

/* ============================================================
   BRONZE LAYER – TABLE DEFINITIONS (DDL)
   ------------------------------------------------------------
   Purpose:
   - Store raw data ingested from CRM and ERP source systems
   - Minimal transformation applied
   - Tables closely mirror source system structures
   ============================================================ */

/* ---------------- CRM SOURCE TABLES ---------------- */

-- Stores raw customer master data from CRM
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

-- Stores raw product master data from CRM
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

-- Stores raw sales transaction data from CRM
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

/* ---------------- ERP SOURCE TABLES ---------------- */

-- Stores customer location data from ERP
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

-- Stores additional customer attributes from ERP
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

-- Stores product category and maintenance details from ERP
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);




/* ============================================================
   BRONZE LAYER – LOAD PROCEDURE
   ------------------------------------------------------------
   Purpose:
   - Perform daily batch ingestion into Bronze layer
   - Uses TRUNCATE + BULK INSERT (full refresh strategy)
   - Logs load duration for each table
   - Designed to be scheduler-friendly
   ============================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    /* --------------------------------------------------------
       Variables to capture load start and end timestamps
       Used to calculate table-level load duration
       -------------------------------------------------------- */
    DECLARE @start_time DATETIME,
            @end_time   DATETIME;

    BEGIN TRY

        /* ====================================================
           High-level execution logs
           ==================================================== */
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        /* ---------------- bronze.crm_cust_info ---------------- */

        -- Capture start time for performance tracking
        SET @start_time = GETDATE();
        PRINT '>> INSERT into bronze.crm_cust_info';

        -- Remove existing data to ensure full refresh
        TRUNCATE TABLE bronze.crm_cust_info;

        -- Load raw CRM customer data from CSV
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\data with baraa prjct\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,          -- Skip header row
            FIELDTERMINATOR = ',', -- CSV delimiter
            TABLOCK                -- Optimize bulk load performance
        );

        -- Capture end time and log duration
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';


        /* ---------------- bronze.crm_prd_info ---------------- */

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
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';


        /* ---------------- bronze.crm_sales_details ---------------- */

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
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';


        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        /* ---------------- bronze.erp_loc_a101 ---------------- */

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
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';


        /* ---------------- bronze.erp_cust_az12 ---------------- */

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
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';


        /* ---------------- bronze.erp_px_cat_g1v2 ---------------- */

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
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------';

    END TRY
    BEGIN CATCH
        /* --------------------------------------------------------
           Error handling block
           Captures and prints detailed error diagnostics
           -------------------------------------------------------- */
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO

/* ============================================================
   Execute Bronze Load
   ------------------------------------------------------------
   This call simulates the daily batch execution
   ============================================================ */
EXEC bronze.load_bronze;
