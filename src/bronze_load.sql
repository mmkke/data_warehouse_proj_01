
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
    BEGIN TRY
        PRINT('=================================================')
        PRINT('LOADING BRONZE LAYER')
        PRINT('=================================================')
        SET @batch_start_time = GETDATE()
        --====================================================================
        -- CRM TABLES
        --====================================================================
        PRINT('LOADING CRM TABLES')
        PRINT('-------------------------------------------------')


        ----------------------------------------------------------
        -- TABLE 1: bronze.crm_cust_info
        ----------------------------------------------------------
        SET @start_time = GETDATE();
        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: crm_cust_info')
        TRUNCATE TABLE bronze.crm_cust_info;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: crm_cust_info')
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        ----------------------------------------------------------
        -- TABLE 2: bronze.crm_prd_info
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: crm_prd_info')
        TRUNCATE TABLE bronze.crm_prd_info;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: crm_prd_info')
        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        ----------------------------------------------------------
        -- TABLE 3: bronze.crm_sales_details
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: crm_sales_details')
        TRUNCATE TABLE bronze.crm_sales_details;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: crm_sales_details')
        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        --====================================================================
        -- CRM TABLES
        --====================================================================
        PRINT('LOADING ERP TABLES')
        PRINT('-------------------------------------------------')
        ----------------------------------------------------------
        -- TABLE 4: bronze.erp_cust_az12
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: erp_cust_az12')
        TRUNCATE TABLE bronze.erp_cust_az12;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: erp_cust_az12')
        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        ----------------------------------------------------------
        -- TABLE 5: bronze.erp_loc_a101
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: erp_loc_a101')
        TRUNCATE TABLE bronze.erp_loc_a101;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: erp_loc_a101')
        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/datasets/source_erp/loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        ----------------------------------------------------------
        -- TABLE 6: bronze.erp_px_cat_g1v2
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        -- STEP 1: CLEAR TABLE
        PRINT('>> TRUNCATING TABLE: erp_px_cat_g1v2')
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        -- STEP 2: FULL LOAD FROM CSV
        PRINT('>> BULK LOADING DATA INTO: erp_px_cat_g1v2')
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds.'
        PRINT('-------------------------------------------------')

        -- Compute Total Time
        SET @batch_end_time = GETDATE()
        PRINT '>> Total Bronze Layer Loading Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.'

    END TRY
    BEGIN CATCH
        PRINT('-------------------------------------------------')
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
        PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE(); 
        PRINT 'ERORR NUMBER:' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '-------------------------------------------------'
    END CATCH
END

