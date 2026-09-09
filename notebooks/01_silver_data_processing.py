# %%
##Reading Transaction data from synapses storage account
retail_transactions_data = ("abfss://<container>@<storage-account>.dfs.core.windows.net/p1retail/bronze/transactions")
trans_df = spark.read.options(header = 'true',inferSchema = 'true').csv(retail_transactions_data)

# %%
#Understanding the data
total_rows = trans_df.count()
unique_rows = trans_df.dropDuplicates().count()

duplicate_count = total_rows - unique_rows

print("Total rows:", total_rows)
print("Duplicate rows:", duplicate_count)
print("\nData Schema:")
trans_df.printSchema()
print("Total columns:", len(trans_df.columns))

# %% [markdown]
# The invoicedate need to be converted to timedatestamp and the duplicate rows to be dropped

# %%
#Checking the InvoiceDate original format
trans_df.select("InvoiceDate").show(20, truncate=False)

# %%
from pyspark.sql.functions import col, to_timestamp

trans_df = trans_df.withColumn(
    "InvoiceDate",
    to_timestamp(col("InvoiceDate"), "dd-MM-yyyy HH:mm")
)

trans_df.printSchema()
trans_df.select("InvoiceDate").show(10, truncate=False)

# %% [markdown]
# Null value check

# %%
from pyspark.sql.functions import col, sum as spark_sum

null_counts = trans_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in trans_df.columns
])

display(null_counts)

# %% [markdown]
# Description and customer id contains null values. As the description column is not required for analysis so removed
# Transactions with a missing customer ID be used for product,revenue,inventory and geographical analysis so we retain it

# %%
from pyspark.sql.functions import col, when

# Remove Description because Product Master will provide product details
trans_df = trans_df.drop("Description")

# Add a flag to identify transactions with a known customer
trans_df = trans_df.withColumn(
    "CustomerKnown",
    when(col("Customer ID").isNull(), False).otherwise(True)
)

# Verify the result
trans_df.printSchema()

print(
    "Rows with missing Customer ID:",
    trans_df.filter(col("Customer ID").isNull()).count()
)

display(
    trans_df.limit(10)
)

# %% [markdown]
# Duplicate records

# %%
# Create the clean dataset containing one copy of each record
trans_clean_df = trans_df.dropDuplicates()

# Identify duplicate record groups
duplicate_df = (
    trans_df
    .groupBy(trans_df.columns)
    .count()
    .filter("count > 1")
)

print("Original row count:", trans_df.count())
print("Clean row count:", trans_clean_df.count())

duplicate_count = trans_df.count() - trans_clean_df.count()
print("Duplicate rows to be removed:", duplicate_count)

display(duplicate_df)

# %%
display(trans_clean_df.limit(5))

# %%
from pyspark.sql.functions import col

print(
    "Negative quantity rows:",
    trans_clean_df.filter(col("Quantity") < 0).count()
)

print(
    "Zero quantity rows:",
    trans_clean_df.filter(col("Quantity") == 0).count()
)

print(
    "Negative price rows:",
    trans_clean_df.filter(col("Price") < 0).count()
)

print(
    "Zero price rows:",
    trans_clean_df.filter(col("Price") == 0).count()
)

cancelled_df = trans_clean_df.filter(
    col("Invoice").startswith("C")
)

print(
    "Cancelled transaction rows:",
    cancelled_df.count()
)

# %%
from pyspark.sql.functions import col

# Cancelled invoice AND negative quantity
cancelled_negative = trans_clean_df.filter(
    (col("Invoice").startswith("C")) &
    (col("Quantity") < 0)
).count()

# Negative quantity but NOT cancelled
negative_not_cancelled = trans_clean_df.filter(
    (~col("Invoice").startswith("C")) &
    (col("Quantity") < 0)
).count()

# Cancelled invoice but quantity is NOT negative
cancelled_not_negative = trans_clean_df.filter(
    (col("Invoice").startswith("C")) &
    (col("Quantity") >= 0)
).count()

print("Cancelled + Negative Quantity:", cancelled_negative)
print("Negative Quantity but NOT Cancelled:", negative_not_cancelled)
print("Cancelled but NOT Negative Quantity:", cancelled_not_negative)

# %%
from pyspark.sql.functions import col, when

trans_clean_df = trans_clean_df.withColumn(
    "TransactionType",
    when(
        col("Invoice").startswith("C"),
        "Cancellation"
    )
    .when(
        col("Quantity") < 0,
        "Adjustment_Return"
    )
    .otherwise("Sale")
)

display(
    trans_clean_df.groupBy("TransactionType").count()
)

# %%
display(
    trans_clean_df.select(
        "Invoice",
        "InvoiceDate",
        "StockCode",
        "Quantity",
        "Price",
        "TransactionType"
    ).filter(
        col("TransactionType") != "Sale"
    ).limit(20)
)

# %% [markdown]
# Price is a critical field because it will be used to calculate sales revenue.
# 
# The profiling identified transactions with zero and negative prices. These records will be investigated before applying a cleansing rule.
# 
# Zero-priced transactions may represent free items, promotional items, adjustments, or incomplete pricing information. Negative prices are unusual for normal retail transactions and may represent accounting adjustments or data-quality issues.
# 
# These records will therefore be separated and reviewed before determining whether they should be included in sales revenue analysis.

# %%
# Separate invalid or unusual price records
price_issue_df = trans_clean_df.filter(
    col("Price") <= 0
)

print("Total price issue records:", price_issue_df.count())
print(
    "Negative price records:",
    price_issue_df.filter(col("Price") < 0).count()
)
print(
    "Zero price records:",
    price_issue_df.filter(col("Price") == 0).count()
)

display(
    price_issue_df.select(
        "Invoice",
        "StockCode",
        "Quantity",
        "Price",
        "TransactionType",
        "Customer ID",
        "Country"
    )
)

# %%
display(
    trans_clean_df
    .filter(col("Price") < 0)
    .select(
        "Invoice",
        "StockCode",
        "Quantity",
        "Price",
        "TransactionType",
        "Customer ID",
        "Country"
    )
)

# %%
display(
    trans_clean_df
    .filter(col("Price") == 0)
    .groupBy("StockCode")
    .count()
    .orderBy(col("count").desc())
)

# %%
# Store negative-price records separately
invalid_price_df = trans_clean_df.filter(
    col("Price") < 0
)

# Continue Silver processing without negative-price records
trans_clean_df = trans_clean_df.filter(
    col("Price") >= 0
)

print("Invalid negative-price records:", invalid_price_df.count())
print("Remaining transaction records:", trans_clean_df.count())

display(trans_clean_df.limit(5))

# %%
# Base Silver path
silver_base_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/silver/transactions"
)

# Define output paths
silver_transactions_path = f"{silver_base_path}/clean"
duplicate_path = f"{silver_base_path}/data_quality/duplicates"
invalid_price_path = f"{silver_base_path}/data_quality/invalid_price"

# %%
(
    trans_clean_df
    .write
    .mode("overwrite")
    .parquet(silver_transactions_path)
)

print("Clean transaction data written to:")
print(silver_transactions_path)

# %%
silver_trans_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/silver/transactions/clean/"
)

silver_trans_df = spark.read.parquet(silver_trans_path)

# %%
silver_trans_df.printSchema()
from pyspark.sql.functions import min, max

silver_trans_df.select(
    min("InvoiceDate").alias("MinInvoiceDate"),
    max("InvoiceDate").alias("MaxInvoiceDate")
).show()

# %%
(
    duplicate_df
    .write
    .mode("overwrite")
    .parquet(duplicate_path)
)

print("Duplicate records written to:")
print(duplicate_path)

# %%
(
    invalid_price_df
    .write
    .mode("overwrite")
    .parquet(invalid_price_path)
)

print("Invalid price records written to:")
print(invalid_price_path)

# %%
customer_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/bronze/customer/"
)

product_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/bronze/products/"
)

inventory_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/bronze/inventory/"
)

# %%
customer_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(customer_path)
)

print("Customer rows:", customer_df.count())
print("Customer columns:", len(customer_df.columns))

customer_df.printSchema()
display(customer_df.limit(10))

# %%
from pyspark.sql.functions import col, sum as spark_sum

# 1. Null values
customer_nulls = customer_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in customer_df.columns
])

display(customer_nulls)

# %%
# 2. Exact duplicate rows
customer_rows = customer_df.count()
customer_unique_rows = customer_df.dropDuplicates().count()

print("Customer rows:", customer_rows)
print("Duplicate rows:", customer_rows - customer_unique_rows)

# %%
# 4. Numeric checks

print(
    "Negative OrderCount:",
    customer_df.filter(col("OrderCount") < 0).count()
)

print(
    "Negative ObservedQuantity:",
    customer_df.filter(col("ObservedQuantity") < 0).count()
)

print(
    "Negative ObservedRevenueGBP:",
    customer_df.filter(col("ObservedRevenueGBP") < 0).count()
)

# %%
# 6. Validate categorical fields

display(
    customer_df.groupBy("CustomerSegment").count()
)

display(
    customer_df.groupBy("LoyaltyTier").count()
)

display(
    customer_df.groupBy("PreferredChannel").count()
)

display(
    customer_df.groupBy("MarketingOptIn").count()
)

# %%
product_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(product_path)
)

print("Product rows:", product_df.count())
print("Product columns:", len(product_df.columns))

product_df.printSchema()
display(product_df.limit(10))

# %%
#checking the distinct productnames
display(
    product_df
    .select("StockCode", "ProductName")
    .distinct()
    .orderBy("StockCode")
)

# %%
from pyspark.sql.functions import col, sum as spark_sum

product_nulls = product_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in product_df.columns
])

display(product_nulls)

# %%
product_df.select(
    "MedianUnitPriceGBP",
    "ObservedSalesQuantity",
    "TransactionLineCount"
).summary(
    "count",
    "min",
    "25%",
    "50%",
    "75%",
    "max"
).show(truncate=False)

# %%
from pyspark.sql.functions import col
display(
    product_df
    .select(
        "StockCode",
        "ProductName",
        "Category",
        "MedianUnitPriceGBP",
        "ObservedSalesQuantity",
        "TransactionLineCount"
    )
    .orderBy(col("MedianUnitPriceGBP").desc())
    .limit(50)
)

# %%
from pyspark.sql.functions import col, count, sum as spark_sum, abs as spark_abs

cancel_stats = (
    silver_trans_df
    .filter(col("TransactionType") == "Cancellation")
    .groupBy("StockCode")
    .agg(
        count("*").alias("CancellationLineCount"),
        spark_sum(
            spark_abs(col("Quantity"))
        ).alias("CancelledQuantity")
    )
)

cancelled_product_profile = (
    product_df
    .join(cancel_stats, "StockCode", "inner")
    .select(
        "StockCode",
        "ProductName",
        "Category",
        "MedianUnitPriceGBP",
        "ObservedSalesQuantity",
        "TransactionLineCount",
        "CancellationLineCount",
        "CancelledQuantity"
    )
)

display(
    cancelled_product_profile
    .orderBy(col("CancellationLineCount").desc())
)

# %%
from pyspark.sql.functions import col

alphabetic_stockcodes = (
    product_df
    .filter(col("StockCode").rlike("^[A-Za-z]+$"))
    .select(
        "StockCode",
        "ProductName",
        "Category",
        "MedianUnitPriceGBP",
        "ObservedSalesQuantity",
        "TransactionLineCount"
    )
    .distinct()
    .orderBy("StockCode")
)

display(alphabetic_stockcodes)

# %%
from pyspark.sql.functions import col, upper, when

non_merchandise_codes = [
    "ADJUST",
    "AMAZONFEE",
    "B",
    "CRUK",
    "D",
    "DOT",
    "M",
    "POST",
    "S","m","GIFT","BANK CHARGES"
]

product_check = product_df.withColumn(
    "ProductType",
    when(
        upper(col("StockCode")).isin(non_merchandise_codes),
        "Non-Merchandise"
    ).otherwise("Merchandise")
)

display(
    product_check
    .select(
        "StockCode",
        "ProductName",
        "Category",
        "ProductType"
    )
    .filter(col("ProductType") == "Non-Merchandise")
    .orderBy("StockCode")
)

# %%
product_rows = product_check.count()
product_unique_rows = product_check.dropDuplicates().count()

print("Product rows:", product_rows)
print(
    "Exact duplicate rows:",
    product_rows - product_unique_rows
)

# %%
inventory_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(inventory_path)
)

print("Inventory rows:", inventory_df.count())
print("Inventory columns:", len(inventory_df.columns))

inventory_df.printSchema()
display(inventory_df.limit(10))

# %%
from pyspark.sql.functions import col, count, sum as spark_sum, abs as spark_abs
inventory_nulls = inventory_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in inventory_df.columns
])

display(inventory_nulls)

# %%
# 1. Null values
inventory_nulls = inventory_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in inventory_df.columns
])

display(inventory_nulls)

# 2. Exact duplicate rows
inventory_rows = inventory_df.count()
inventory_unique_rows = inventory_df.dropDuplicates().count()

print("Inventory rows:", inventory_rows)
print(
    "Exact duplicate rows:",
    inventory_rows - inventory_unique_rows
)

# 3. Duplicate inventory observations
duplicate_inventory_keys = (
    inventory_df
    .groupBy(
        "SnapshotMonth",
        "StockCode",
        "WarehouseID"
    )
    .count()
    .filter(col("count") > 1)
)

print(
    "Duplicate inventory keys:",
    duplicate_inventory_keys.count()
)

display(duplicate_inventory_keys)

# 4. Quantity validation
print(
    "Negative QuantityOnHand:",
    inventory_df.filter(col("QuantityOnHand") < 0).count()
)

print(
    "Negative ReorderLevel:",
    inventory_df.filter(col("ReorderLevel") < 0).count()
)

print(
    "Negative SafetyStock:",
    inventory_df.filter(col("SafetyStock") < 0).count()
)

print(
    "Negative DemandQtyPreviousMonth:",
    inventory_df.filter(col("DemandQtyPreviousMonth") < 0).count()
)

# 5. Lead time validation
print(
    "Negative LeadTimeDays:",
    inventory_df.filter(col("LeadTimeDays") < 0).count()
)

# 6. Inventory status distribution
display(
    inventory_df
    .groupBy("InventoryStatus")
    .count()
    .orderBy(col("count").desc())
)

# 7. Warehouse distribution
display(
    inventory_df
    .groupBy("WarehouseID", "WarehouseCity")
    .count()
    .orderBy("WarehouseID")
)

# %%
silver_customer_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/silver/customer/"
)

silver_product_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/silver/products/"
)

silver_inventory_path = (
    "abfss://<container>@<storage-account>.dfs.core.windows.net/"
    "p1retail/silver/inventory/"
)

# %%
product_check.write \
    .mode("overwrite") \
    .parquet(silver_product_path)

# %%
customer_df.write \
    .mode("overwrite") \
    .parquet(silver_customer_path)

# %%
inventory_df.write \
    .mode("overwrite") \
    .parquet(silver_inventory_path)
