# Architecture

The implemented architecture follows this pattern:

```text
Transactions | Products | Customers | Inventory
                    |
                    v
             ADLS Gen2 Bronze
                    |
                    v
          Azure Synapse Spark Pool
        Profiling / Cleaning / Validation
                    |
                    v
             ADLS Gen2 Silver
                    |
                    v
      Synapse Serverless SQL Pool
      Views / Joins / Aggregations / CETAS
                    |
                    v
              ADLS Gen2 Gold
                    |
                    v
                 Power BI
```

Azure Synapse Pipelines provide orchestration and monitoring across the processing stages.

Architecture diagrams and redacted screenshots can be added to this directory.