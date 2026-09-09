-- Objective 1: Product and category performance by month and year

CREATE EXTERNAL TABLE gold.ProductPerformance
WITH
(
    LOCATION = 'gold/product_performance/',
    DATA_SOURCE = [RetailLake],
    FILE_FORMAT = [ParquetFormat]
)
AS
SELECT
    YEAR(t.InvoiceDate) AS SalesYear,
    MONTH(t.InvoiceDate) AS SalesMonth,
    t.StockCode,
    p.ProductName,
    p.Category,
    SUM(CAST(t.Quantity AS BIGINT)) AS QuantitySold,
    SUM(
        CAST(t.Quantity AS DECIMAL(18,2)) *
        CAST(t.Price AS DECIMAL(18,2))
    ) AS RevenueGBP,
    COUNT(DISTINCT t.Invoice) AS OrderCount
FROM silver.vw_transactions t
LEFT JOIN silver.vw_products p
    ON t.StockCode = p.StockCode
WHERE
    t.TransactionType = 'Sale'
    AND t.Price > 0
GROUP BY
    YEAR(t.InvoiceDate),
    MONTH(t.InvoiceDate),
    t.StockCode,
    p.ProductName,
    p.Category;
GO

SELECT TOP 20 *
FROM gold.ProductPerformance;
