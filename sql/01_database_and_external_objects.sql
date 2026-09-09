-- Project 1: UK Retail Azure Data Engineering
-- Synapse Serverless SQL database and Silver external objects

CREATE DATABASE RetailAnalytics;
GO

USE RetailAnalytics;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

CREATE EXTERNAL DATA SOURCE RetailLake
WITH
(
    LOCATION = 'abfss://projectdata@synapsesanju.dfs.core.windows.net/p1retail'
);
GO

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH
(
    FORMAT_TYPE = PARQUET
);
GO

CREATE OR ALTER VIEW silver.vw_transactions
AS
SELECT
    Invoice,
    StockCode,
    Quantity,
    InvoiceDate,
    Price,
    [Customer ID],
    Country,
    CustomerKnown,
    TransactionType
FROM OPENROWSET
(
    BULK 'silver/transactions/clean/*.parquet',
    DATA_SOURCE = 'RetailLake',
    FORMAT = 'PARQUET'
)
WITH
(
    Invoice          VARCHAR(50),
    StockCode        VARCHAR(50),
    Quantity         INT,
    InvoiceDate      DATETIME2,
    Price            FLOAT,
    [Customer ID]    INT,
    Country          VARCHAR(100),
    CustomerKnown    BIT,
    TransactionType  VARCHAR(50)
) AS t;
GO

CREATE OR ALTER VIEW silver.vw_products
AS
SELECT *
FROM OPENROWSET
(
    BULK 'silver/products/*.parquet',
    DATA_SOURCE = 'RetailLake',
    FORMAT = 'PARQUET'
) AS p;
GO

CREATE OR ALTER VIEW silver.vw_customers
AS
SELECT *
FROM OPENROWSET
(
    BULK 'silver/customer/*.parquet',
    DATA_SOURCE = 'RetailLake',
    FORMAT = 'PARQUET'
) AS c;
GO

CREATE OR ALTER VIEW silver.vw_inventory
AS
SELECT *
FROM OPENROWSET
(
    BULK 'silver/inventory/*.parquet',
    DATA_SOURCE = 'RetailLake',
    FORMAT = 'PARQUET'
) AS i;
GO
