#!/usr/bin/env python3
"""
Create / Recreate DataWarehouse01 Database and Schemas (Python Version)
======================================================================

This script connects to a SQL Server instance and:

1. Connects to the `master` database.
2. Drops the `DataWarehouse01` database if it exists (forcing single-user mode).
3. Creates a fresh `DataWarehouse01` database.
4. Creates the `bronze`, `silver`, and `gold` schemas inside it.

WARNING:
    This will DROP the existing 'DataWarehouse01' database if it exists.
"""

import os
import pyodbc


def get_connection(database="master"):
    """
    Create a connection to SQL Server using environment variables:
        DB_SERVER, DB_USER, DB_PASSWORD
    """
    server = os.getenv("DB_SERVER", "localhost,1433")
    user = os.getenv("DB_USER", "sa")
    password = os.getenv("DB_PASSWORD", "YourStrongPassword123!")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    ).format(driver=driver)

    # autocommit is important for CREATE/DROP DATABASE
    return pyodbc.connect(conn_str, autocommit=True)


def drop_and_create_database():
    conn = get_connection(database="master")
    cursor = conn.cursor()

    print("Dropping DataWarehouse01 if it exists...")
    cursor.execute(
        """
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse01')
        BEGIN 
            ALTER DATABASE DataWarehouse01 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE DataWarehouse01;
        END;
        """
    )

    print("Creating DataWarehouse01...")
    cursor.execute("CREATE DATABASE DataWarehouse01;")

    cursor.close()
    conn.close()


def create_schemas():
    # Now connect directly to the new database
    conn = get_connection(database="DataWarehouse01")
    cursor = conn.cursor()

    print("Creating schemas (bronze, silver, gold) if they do not exist...")

    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
            EXEC('CREATE SCHEMA bronze');
        """
    )
    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
            EXEC('CREATE SCHEMA silver');
        """
    )
    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
            EXEC('CREATE SCHEMA gold');
        """
    )

    cursor.close()
    conn.close()


def main():
    print("Starting database and schema setup...")
    drop_and_create_database()
    create_schemas()
    print("Done. DataWarehouse01 and schemas created.")


if __name__ == "__main__":
    main()