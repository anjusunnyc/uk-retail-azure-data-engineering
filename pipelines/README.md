# Azure Synapse Pipelines

This folder documents the orchestration layer used in the UK Retail data engineering project.

The Synapse pipeline coordinates the movement and processing of data through the Medallion architecture:

```text
Bronze source data
      |
      v
Synapse Spark processing
      |
      v
Silver curated Parquet
      |
      v
Synapse Serverless SQL Gold processing
      |
      v
Gold analytical datasets
      |
      v
Power BI
```

The actual Synapse workspace pipeline artifact can be added here later if it is exported from Azure Synapse Studio as source-controlled JSON.