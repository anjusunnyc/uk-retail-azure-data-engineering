-- Objective 2: Customer purchasing behavior

CREATE OR ALTER VIEW gold.vw_CustomerAnalytics
AS
SELECT
    t.[Customer ID] AS CustomerID,
    c.Country,
    c.CustomerSegment,
    c.LoyaltyTier,
    c.PreferredChannel,
    COUNT(DISTINCT t.Invoice) AS OrderCount,
    SUM(CAST(t.Quantity AS BIGINT)) AS TotalQuantityPurchased,
    SUM(
        CAST(t.Quantity AS DECIMAL(18,2)) *
        CAST(t.Price AS DECIMAL(18,2))
    ) AS TotalSpendGBP,
    CAST(
        SUM(
            CAST(t.Quantity AS DECIMAL(18,2)) *
            CAST(t.Price AS DECIMAL(18,2))
        ) / NULLIF(COUNT(DISTINCT t.Invoice), 0)
        AS DECIMAL(18,2)
    ) AS AverageOrderValueGBP,
    MIN(t.InvoiceDate) AS FirstPurchaseDate,
    MAX(t.InvoiceDate) AS LastPurchaseDate
FROM silver.vw_transactions t
LEFT JOIN silver.vw_customers c
    ON t.[Customer ID] = c.CustomerID
WHERE
    t.TransactionType = 'Sale'
    AND t.Price > 0
    AND t.[Customer ID] IS NOT NULL
GROUP BY
    t.[Customer ID],
    c.Country,
    c.CustomerSegment,
    c.LoyaltyTier,
    c.PreferredChannel;
GO

CREATE EXTERNAL TABLE gold.CustomerAnalytics
WITH
(
    LOCATION = 'gold/customer_analytics/',
    DATA_SOURCE = [RetailLake],
    FILE_FORMAT = [ParquetFormat]
)
AS
SELECT
    CustomerID,
    Country,
    CustomerSegment,
    LoyaltyTier,
    PreferredChannel,
    OrderCount,
    TotalQuantityPurchased AS QuantityPurchased,
    TotalSpendGBP AS TotalRevenueGBP,
    AverageOrderValueGBP
FROM gold.vw_CustomerAnalytics;
GO
