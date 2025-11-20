/*
==========================================================================================================
Crete Database and Schema
==========================================================================================================

Purpose:
    This script creates and new database named 'DataWarehouse01' after 
    checking if it already exists. If it already exists the current 
    database will be dropped and recreated. Additonally, this script sets 
    up the three schemas withing the database:
        * 'bronze'
        * 'silver
        * 'gold

WARNING: THIS SCRIPT WILL DROP THE CURRENT 'DataWarehouse01' WAREHOUSE IF IT ALREADY EXISTS. 

*/

USE master;
GO

--Drop and recreate the 'DataWarehouse01' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse01')
BEGIN 
    ALTER DATABASE DataWarehouse01 SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE DataWarehouse01;
END;
GO

--Create Database
CREATE DATABASE DataWarehouse01;
GO

-- Set to new DB
USE DataWarehouse01;
GO
-- CREATE SCHEMAs

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;