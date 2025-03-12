/*
=============================================
Create Database and Schemas
=============================================

This script creates a new database and checks if it already exists.
The script setup three schemas within the database: bronze, silver and gold


USE MASTER;
GO
CREATE DATABASE DataWarehouse27022025;

USE DataWarehouse27022025;

--Create schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA gold;
GO
