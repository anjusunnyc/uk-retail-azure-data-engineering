-- Objective 5: Product profitability analysis

CREATE OR ALTER VIEW gold.vw_ProductProfitability
AS
SELECT
    t.StockCode,
    p.ProductName,
    p.Category,
    SUM(CAST(t.Quantity AS BIGINT)) AS QuantitySold,
    SUM(
        CAST(t.Quantity AS DECIMAL(18,2)) *
        CAST(t.Price AS DECIMAL(18,2))
    ) AS RevenueGBP,
    SUM(
        CAST(t.Quantity AS DECIMAL(18,2)) *
        CAST(p.EstimatedUnitCostGBP AS DECIMAL(18,2))
    ) AS EstimatedCostGBP,
    SUM(
        CAST(t.Quantity AS DECIMAL(18,2)) *
        (
            CAST(t.Price AS DECIMAL(18,2)) -
            CAST(p.EstimatedUnitCostGBP AS DECIMAL(18,2))
        )
    ) AS EstimatedGrossProfitGBP,
    CAST(
        100.0 *
        SUM(
            CAST(t.Quantity AS DECIMAL(18,2)) *
            (
                CAST(t.Price AS DECIMAL(18,2)) -
                CAST(p.EstimatedUnitCostGBP AS DECIMAL(18,2))
            )
        )
        /
        NULLIF(
            SUM(
                CAST(t.Quantity AS DECIMAL(18,2)) *
                CAST(t.Price AS DECIMAL(18,2))
            ),
            0
        )
        AS DECIMAL(10,2)
    ) AS GrossMarginPct
FROM silver.vw_transactions t
INNER JOIN silver.vw_products p
    ON t.StockCode = p.StockCode
WHERE
    t.TransactionType = 'Sale'
    AND t.Price > 0
    AND p.ProductType = 'Merchandise'
    AND p.EstimatedUnitCostGBP IS NOT NULL
GROUP BY
    t.StockCode,
    p.ProductName,
    p.Category;
GO

CREATE EXTERNAL TABLE gold.ProductProfitability
WITH
(
    LOCATION = 'gold/product_profitability/',
    DATA_SOURCE = [RetailLake],
    FILE_FORMAT = [ParquetFormat]
)
AS
SELECT
    StockCode,
    ProductName,
    Category,
    QuantitySold,
    RevenueGBP,
    EstimatedCostGBP,
    EstimatedGrossProfitGBP,
    GrossMarginPct
FROM gold.vw_ProductProfitability;
GO
