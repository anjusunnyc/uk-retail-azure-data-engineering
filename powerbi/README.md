# Power BI

Power BI is the presentation and semantic-model layer for the UK Retail Azure Data Engineering project.

Power BI consumes business-ready Gold datasets produced by Synapse Serverless SQL, including:

- Product Performance
- Customer Analytics
- Cancellation Analysis
- Inventory Analysis
- Product Profitability

The report supports analysis of revenue, product performance, customer behavior, cancellation rates, inventory risk, and profitability.

The Power BI model uses shared dimensions where appropriate so filters such as product and category can be applied consistently across analytical tables.

The `.pbix` file is not included automatically in this repository. It can be added manually if publishing the report file is appropriate and its size/data contents have been reviewed first.