# UK Retail Azure Data Engineering

## End-to-End Retail Data Engineering Project using Azure Synapse Analytics

This project implements a cloud-based retail data platform on Microsoft Azure for a UK retail business. The solution ingests transaction, product, customer, and inventory data, processes and integrates the data using Azure Synapse Analytics, stores curated datasets in Azure Data Lake Storage Gen2, and serves business-ready outputs to Power BI.

The project focuses on both data engineering and business analytics. The engineering goal is to build a centralized, maintainable Bronze-Silver-Gold data platform. The business goal is to improve sales visibility, customer analysis, cancellation analysis, inventory decisions, and product profitability analysis.

## Project Story

Tek Data Solutions is supporting a UK-based retailer that sells household, gift, and lifestyle products through retail and online channels. As the retailer grew, transaction, product, customer, and inventory data became distributed across different sources and formats. The platform was designed to centralize these datasets, clean and integrate them, and provide reliable analytical outputs.

## Business Objectives

1. Identify the top 10 products and categories by revenue and quantity sold, and analyze monthly performance.
2. Analyze customer purchasing behavior to identify high-value customers and understand ordering frequency, spending, and purchasing patterns.
3. Analyze product cancellation patterns to identify products and categories with the highest cancellation rates and assess their impact on sales revenue.
4. Compare monthly product demand with available inventory to identify stock-out risk and excess inventory.
5. Analyze product and category profitability to identify high-profit products, low-margin products, and opportunities to improve pricing and inventory allocation.

## Data Sources

The project uses four main datasets:

- UCI Online Retail II transactions: 1,067,371 transaction-line records.
- Product Master: 5,304 products.
- Customer Enrichment: 5,942 identified customers.
- Monthly Inventory: 397,800 monthly warehouse-product snapshots.

The transaction dataset is the primary source. Product, customer, and inventory datasets use the same business keys and include both UCI-derived and clearly identified synthetic enrichment fields.

## Solution Architecture

```text
Data Sources
    |
    v
ADLS Gen2 - Bronze / Raw
    |
    v
Azure Synapse Spark Pool
Data Profiling, Cleansing, Standardization
    |
    v
ADLS Gen2 - Silver / Clean
    |
    v
Azure Synapse Serverless SQL Pool
Integration, Views, Aggregations, CETAS
    |
    v
ADLS Gen2 - Gold / Business Ready
    |
    v
Power BI

Azure Synapse Pipelines provide orchestration and monitoring across the flow.
```

## Technology Stack

| Area | Technology |
|---|---|
| Cloud | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Synapse Analytics Spark Pool |
| SQL Serving | Azure Synapse Serverless SQL Pool |
| Orchestration | Azure Synapse Pipelines |
| File Format | Parquet |
| BI | Power BI |
| Source Control | GitHub |

## Medallion Architecture

### Bronze
Raw source files are stored in ADLS Gen2 with minimal modification.

### Silver
The Spark processing notebook performs profiling, cleansing, validation, standardization, and preparation of curated Parquet datasets for transactions, products, customers, and inventory.

### Gold
Synapse Serverless SQL reads Silver Parquet files through external views and creates business-ready analytical outputs for the five project objectives.

## Silver Processing

The Spark notebook in `notebooks/01_silver_data_processing.ipynb` is responsible for the main data-processing stage. It performs data-quality checks and prepares curated Silver datasets before downstream SQL analytics.

Key transformation areas include:

- Reading Bronze datasets.
- Profiling row counts and schema.
- Checking null and invalid values.
- Cleaning transaction records.
- Distinguishing sales, cancellations, and other transaction types.
- Preparing product, customer, and inventory datasets.
- Writing cleaned Parquet files to Silver paths in ADLS Gen2.

## Synapse Serverless SQL Layer

The SQL implementation creates:

- Database: `RetailAnalytics`
- Schemas: `silver` and `gold`
- External data source pointing to the retail data lake
- Parquet external file format
- Silver views over transactions, products, customers, and inventory
- Gold analytical views and external tables

### Silver Views

```text
silver.vw_transactions
silver.vw_products
silver.vw_customers
silver.vw_inventory
```

## Gold Analytics

### 1. Product Performance

The product-performance Gold dataset calculates:

- Sales year
- Sales month
- Product
- Category
- Quantity sold
- Revenue
- Order count

Output:

```text
gold.ProductPerformance
```

### 2. Customer Analytics

The customer Gold dataset calculates:

- Customer ID
- Country
- Customer segment
- Loyalty tier
- Preferred channel
- Order count
- Total quantity purchased
- Total spend
- Average order value
- First and last purchase dates

Output:

```text
gold.CustomerAnalytics
```

### 3. Cancellation Analysis

The cancellation Gold dataset measures:

- Sold quantity
- Cancelled quantity
- Sales revenue
- Cancelled revenue
- Cancellation rate percentage

The analysis is restricted to merchandise products.

Output:

```text
gold.CancellationAnalysis
```

### 4. Inventory Analysis

Monthly sales demand is aligned with monthly inventory snapshots. The analysis derives:

- Monthly demand
- Quantity on hand
- Reorder level
- Safety stock
- Lead time
- Stock coverage ratio
- Inventory position

Inventory positions include:

```text
NO_DEMAND
STOCKOUT_RISK
EXCESS_INVENTORY
HEALTHY
```

Output:

```text
gold.InventoryAnalysis
```

### 5. Product Profitability

The profitability dataset calculates:

- Quantity sold
- Revenue
- Estimated cost
- Estimated gross profit
- Gross margin percentage

The calculation uses the estimated unit cost field from the Product Master dataset.

Output:

```text
gold.ProductProfitability
```

## Power BI Semantic Model

The Power BI model uses a shared product dimension to relate the Gold analytical datasets. This supports consistent filtering by product and category across product performance, profitability, cancellations, inventory, and customer-facing analysis.

The dashboard includes KPIs such as:

- Total Revenue
- Average Orders per Customer
- Cancellation Rate
- Average Stockout Risk
- Gross Margin Percentage

It also includes visuals for:

- Top 10 products
- Monthly revenue trend
- Monthly demand vs inventory
- Customer segment analysis
- Cancellation rate vs sales revenue
- Product and category profitability

## Repository Structure

```text
uk-retail-azure-data-engineering/
|
+-- README.md
|
+-- notebooks/
|   +-- 01_silver_data_processing.ipynb
|
+-- sql/
|   +-- 01_database_and_external_objects.sql
|   +-- 02_gold_product_performance.sql
|   +-- 03_gold_customer_analytics.sql
|   +-- 04_gold_cancellation_analysis.sql
|   +-- 05_gold_inventory_analysis.sql
|   +-- 06_gold_product_profitability.sql
|   +-- 07_security_and_access.sql
|
+-- pipelines/
|   +-- README.md
|
+-- powerbi/
|   +-- README.md
|
+-- docs/
|   +-- architecture/
|   |   +-- README.md
|   +-- screenshots/
|   |   +-- README.md
|   +-- project-report/
|       +-- README.md
|
+-- .gitignore
```

## Security

Database access was configured through Microsoft Entra identity. Personal tenant/user identifiers are intentionally sanitized in this public repository.

## Important Notes

- Product, customer, and inventory enrichment includes synthetic fields and should be interpreted as simulated enterprise enrichment rather than purely original UCI source data.
- Inventory quantities are synthetic and modeled around observed transaction demand.
- Profitability is based on estimated product cost and therefore represents analytical estimates rather than audited financial results.
- The project uses Synapse Spark for data cleansing and Serverless SQL for integration and Gold analytics.

## Future Enhancements

- Parameterized Synapse Pipelines.
- Automated data-quality validation.
- Incremental ingestion patterns.
- Dedicated metadata/configuration tables.
- CI/CD for Synapse artifacts.
- Forecasting models for demand planning.
- More detailed warehouse-level inventory optimization.
- Automated Power BI deployment and refresh management.

## Conclusion

This project demonstrates an end-to-end Azure data engineering workflow using ADLS Gen2, Azure Synapse Spark, Synapse Serverless SQL, Synapse Pipelines, Parquet, and Power BI. It combines data cleansing, integration, business-rule implementation, analytical modeling, and visualization into a single retail Lakehouse-style solution.