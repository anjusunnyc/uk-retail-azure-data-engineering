-- Objective 3: Product cancellation analysis

CREATE OR ALTER VIEW gold.vw_CancellationAnalysis
AS
SELECT
    t.StockCode,
    p.ProductName,
    p.Category,
    SUM(CASE
        WHEN t.TransactionType = 'Sale' AND t.Price > 0
        THEN CAST(t.Quantity AS BIGINT)
        ELSE 0
    END) AS SoldQuantity,
    SUM(CASE
        WHEN t.TransactionType = 'Cancellation'
        THEN ABS(CAST(t.Quantity AS BIGINT))
        ELSE 0
    END) AS CancelledQuantity,
    SUM(CASE
        WHEN t.TransactionType = 'Sale' AND t.Price > 0
        THEN CAST(t.Quantity AS DECIMAL(18,2)) * CAST(t.Price AS DECIMAL(18,2))
        ELSE 0
    END) AS SalesRevenueGBP,
    SUM(CASE
        WHEN t.TransactionType = 'Cancellation'
        THEN ABS(CAST(t.Quantity AS DECIMAL(18,2)) * CAST(t.Price AS DECIMAL(18,2)))
        ELSE 0
    END) AS CancelledRevenueGBP,
    CAST(
        100.0 *
        SUM(CASE
            WHEN t.TransactionType = 'Cancellation'
            THEN ABS(CAST(t.Quantity AS BIGINT))
            ELSE 0
        END)
        /
        NULLIF(
            SUM(CASE
                WHEN t.TransactionType IN ('Sale', 'Cancellation')
                THEN ABS(CAST(t.Quantity AS BIGINT))
                ELSE 0
            END),
            0
        )
        AS DECIMAL(10,2)
    ) AS CancellationRatePct
FROM silver.vw_transactions t
INNER JOIN silver.vw_products p
    ON t.StockCode = p.StockCode
WHERE p.ProductType = 'Merchandise'
GROUP BY
    t.StockCode,
    p.ProductName,
    p.Category;
GO

CREATE EXTERNAL TABLE gold.CancellationAnalysis
WITH
(
    LOCATION = 'gold/cancellation_analysis/',
    DATA_SOURCE = [RetailLake],
    FILE_FORMAT = [ParquetFormat]
)
AS
SELECT
    StockCode,
    ProductName,
    Category,
    SoldQuantity,
    CancelledQuantity,
    SalesRevenueGBP,
    CancelledRevenueGBP,
    CancellationRatePct
FROM gold.vw_CancellationAnalysis;
GO
