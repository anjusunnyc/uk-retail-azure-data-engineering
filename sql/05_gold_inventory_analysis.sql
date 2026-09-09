-- Objective 4: Monthly demand versus inventory

CREATE OR ALTER VIEW gold.vw_InventoryAnalysis
AS
WITH MonthlyDemand AS
(
    SELECT
        StockCode,
        DATEFROMPARTS(YEAR(InvoiceDate), MONTH(InvoiceDate), 1) AS DemandMonth,
        SUM(CAST(Quantity AS BIGINT)) AS MonthlyDemand
    FROM silver.vw_transactions
    WHERE
        TransactionType = 'Sale'
        AND Price > 0
    GROUP BY
        StockCode,
        YEAR(InvoiceDate),
        MONTH(InvoiceDate)
),
MonthlyInventory AS
(
    SELECT
        StockCode,
        CAST(SnapshotMonth AS DATE) AS SnapshotMonth,
        SUM(CAST(QuantityOnHand AS BIGINT)) AS QuantityOnHand,
        SUM(CAST(ReorderLevel AS BIGINT)) AS ReorderLevel,
        SUM(CAST(SafetyStock AS BIGINT)) AS SafetyStock,
        MAX(LeadTimeDays) AS LeadTimeDays
    FROM silver.vw_inventory
    GROUP BY
        StockCode,
        CAST(SnapshotMonth AS DATE)
)
SELECT
    i.SnapshotMonth,
    i.StockCode,
    p.ProductName,
    p.Category,
    ISNULL(d.MonthlyDemand, 0) AS MonthlyDemand,
    i.QuantityOnHand,
    i.ReorderLevel,
    i.SafetyStock,
    i.LeadTimeDays,
    CAST(
        CASE
            WHEN ISNULL(d.MonthlyDemand, 0) = 0 THEN NULL
            ELSE CAST(i.QuantityOnHand AS DECIMAL(18,2)) /
                 NULLIF(CAST(d.MonthlyDemand AS DECIMAL(18,2)), 0)
        END
        AS DECIMAL(10,2)
    ) AS StockCoverageRatio,
    CASE
        WHEN ISNULL(d.MonthlyDemand, 0) = 0 THEN 'NO_DEMAND'
        WHEN i.QuantityOnHand < d.MonthlyDemand
             OR i.QuantityOnHand <= i.ReorderLevel THEN 'STOCKOUT_RISK'
        WHEN i.QuantityOnHand > (2 * d.MonthlyDemand) THEN 'EXCESS_INVENTORY'
        ELSE 'HEALTHY'
    END AS InventoryPosition
FROM MonthlyInventory i
LEFT JOIN MonthlyDemand d
    ON i.StockCode = d.StockCode
    AND i.SnapshotMonth = d.DemandMonth
INNER JOIN silver.vw_products p
    ON i.StockCode = p.StockCode
WHERE p.ProductType = 'Merchandise';
GO

CREATE EXTERNAL TABLE gold.InventoryAnalysis
WITH
(
    LOCATION = 'gold/inventory_analysis/',
    DATA_SOURCE = [RetailLake],
    FILE_FORMAT = [ParquetFormat]
)
AS
SELECT
    SnapshotMonth,
    StockCode,
    ProductName,
    Category,
    MonthlyDemand,
    QuantityOnHand,
    ReorderLevel,
    SafetyStock,
    LeadTimeDays,
    StockCoverageRatio,
    InventoryPosition
FROM gold.vw_InventoryAnalysis;
GO
