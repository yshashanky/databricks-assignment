# Databricks notebook source
# MAGIC %md
# MAGIC ### Milestone A1: Data Ingestion

# COMMAND ----------

# A1.1 - Load Data with Schema Inference

BASE_PATH = "/Volumes/main/default/dataengineeringassignment/"

df_orders_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_orders_dataset.csv")
df_items_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_order_items_dataset.csv")
df_customers_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_customers_dataset.csv")
df_sellers_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_sellers_dataset.csv")
df_products_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_products_dataset.csv")
df_payments_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_order_payments_dataset.csv")
df_reviews_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_order_reviews_dataset.csv")
df_geo_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "olist_geolocation_dataset.csv")
df_translation_inferred = spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + "product_category_name_translation.csv")


# COMMAND ----------

# A1.2 - Load data with explicit schema

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

# orders schema
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True)
])

# order items schema
items_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True)
])

# customers schema
customers_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True)
])

# sellers schema
sellers_schema = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True)
])

# products schema
products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty", IntegerType(), True),
    StructField("product_weight_g", DoubleType(), True),
    StructField("product_length_cm", DoubleType(), True),
    StructField("product_height_cm", DoubleType(), True),
    StructField("product_width_cm", DoubleType(), True)
])

# payments schema
payments_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True)
])

# reviews schema
reviews_schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True)
])

# geolocation schema
geo_schema = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", DoubleType(), True),
    StructField("geolocation_lng", DoubleType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True)
])

# translation schema
translation_schema = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True)
])

# reload all with explicit schemas
df_orders     = spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").schema(orders_schema).csv(BASE_PATH + "olist_orders_dataset.csv")
df_items      = spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").schema(items_schema).csv(BASE_PATH + "olist_order_items_dataset.csv")
df_customers  = spark.read.option("header", True).schema(customers_schema).csv(BASE_PATH + "olist_customers_dataset.csv")
df_sellers    = spark.read.option("header", True).schema(sellers_schema).csv(BASE_PATH + "olist_sellers_dataset.csv")
df_products   = spark.read.option("header", True).schema(products_schema).csv(BASE_PATH + "olist_products_dataset.csv")
df_payments   = spark.read.option("header", True).schema(payments_schema).csv(BASE_PATH + "olist_order_payments_dataset.csv")
df_reviews    = spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss").schema(reviews_schema).csv(BASE_PATH + "olist_order_reviews_dataset.csv")
df_geo        = spark.read.option("header", True).schema(geo_schema).csv(BASE_PATH + "olist_geolocation_dataset.csv")
df_translation= spark.read.option("header", True).schema(translation_schema).csv(BASE_PATH + "product_category_name_translation.csv")

# COMMAND ----------

# A1.3 - Quick Data Inspection

datasets = {
    "orders":      (df_orders_inferred,      df_orders),
    "items":       (df_items_inferred,        df_items),
    "customers":   (df_customers_inferred,    df_customers),
    "sellers":     (df_sellers_inferred,      df_sellers),
    "products":    (df_products_inferred,     df_products),
    "payments":    (df_payments_inferred,     df_payments),
    "reviews":     (df_reviews_inferred,      df_reviews),
    "geo":         (df_geo_inferred,          df_geo),
    "translation": (df_translation_inferred,  df_translation)
}

for name, (df_inf, df_exp) in datasets.items():
    print("=" * 60)
    print(f"TABLE: {name.upper()}")
    print("-" * 60)
    print("INFERRED SCHEMA:")
    df_inf.printSchema()
    print("EXPLICIT SCHEMA:")
    df_exp.printSchema()
    row_count = df_exp.count()
    col_count = len(df_exp.columns)
    print(f"Rows: {row_count:,}  |  Columns: {col_count}")
    print()

# COMMAND ----------

# A1.3 - Quick Data Inspection (SQL)

# Register all explicit DataFrames as temp views for SQL access
df_orders.createOrReplaceTempView("orders")
df_items.createOrReplaceTempView("items")
df_customers.createOrReplaceTempView("customers")
df_sellers.createOrReplaceTempView("sellers")
df_products.createOrReplaceTempView("products")
df_payments.createOrReplaceTempView("payments")
df_reviews.createOrReplaceTempView("reviews")
df_geo.createOrReplaceTempView("geo")
df_translation.createOrReplaceTempView("translation")

display(spark.sql("""
    SELECT 'orders'      AS table_name, COUNT(*) AS row_count, 8  AS col_count FROM orders      UNION ALL
    SELECT 'items'       AS table_name, COUNT(*) AS row_count, 7  AS col_count FROM items       UNION ALL
    SELECT 'customers'   AS table_name, COUNT(*) AS row_count, 5  AS col_count FROM customers   UNION ALL
    SELECT 'sellers'     AS table_name, COUNT(*) AS row_count, 4  AS col_count FROM sellers     UNION ALL
    SELECT 'products'    AS table_name, COUNT(*) AS row_count, 9  AS col_count FROM products    UNION ALL
    SELECT 'payments'    AS table_name, COUNT(*) AS row_count, 5  AS col_count FROM payments    UNION ALL
    SELECT 'reviews'     AS table_name, COUNT(*) AS row_count, 7  AS col_count FROM reviews     UNION ALL
    SELECT 'geo'         AS table_name, COUNT(*) AS row_count, 5  AS col_count FROM geo         UNION ALL
    SELECT 'translation' AS table_name, COUNT(*) AS row_count, 2  AS col_count FROM translation
"""))

# COMMAND ----------

# A1.4 - File Metadata Table

from datetime import datetime
from pyspark.sql import Row

# Get file metadata from Volumes using dbutils
file_info_list = dbutils.fs.ls(BASE_PATH)

# Row counts for each file
row_counts = {
    "olist_orders_dataset.csv":             df_orders.count(),
    "olist_order_items_dataset.csv":        df_items.count(),
    "olist_customers_dataset.csv":          df_customers.count(),
    "olist_sellers_dataset.csv":            df_sellers.count(),
    "olist_products_dataset.csv":           df_products.count(),
    "olist_order_payments_dataset.csv":     df_payments.count(),
    "olist_order_reviews_dataset.csv":      df_reviews.count(),
    "olist_geolocation_dataset.csv":        df_geo.count(),
    "product_category_name_translation.csv":df_translation.count()
}

load_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

metadata_rows = []
for f in file_info_list:
    fname = f.name
    size_mb = round(f.size / 1024 / 1024, 4)
    size_kb = round(f.size / 1024, 2)
    mod_time = datetime.fromtimestamp(f.modificationTime / 1000).strftime("%Y-%m-%d %H:%M:%S")
    row_count = row_counts.get(fname, 0)
    metadata_rows.append(Row(
        file_name        = fname,
        file_path        = f.path,
        file_size_mb     = size_mb,
        file_size_kb     = size_kb,
        modification_date= mod_time,
        creation_date    = mod_time,
        row_count        = row_count,
        load_timestamp   = load_ts
    ))

df_metadata = spark.createDataFrame(metadata_rows)
df_metadata.createOrReplaceTempView("file_metadata")

print("File Metadata Table:")
display(df_metadata)

# COMMAND ----------

# A1.4 - File Metadata Table (SQL)

display(spark.sql("""
    SELECT
        file_name,
        file_path,
        file_size_mb,
        file_size_kb,
        modification_date,
        creation_date,
        row_count,
        load_timestamp
    FROM file_metadata
    ORDER BY file_size_mb DESC
"""))

# COMMAND ----------

# A1.5 - Exploratory Summary Table

from pyspark.sql.functions import (
    col, count, when, isnan, min as spark_min,
    max as spark_max, desc
)
from pyspark.sql.types import NumericType, TimestampType as TSType, StringType as StrType

# Primary keys per table
primary_keys = {
    "orders":      ["order_id"],
    "items":       ["order_id", "order_item_id"],
    "customers":   ["customer_id"],
    "sellers":     ["seller_id"],
    "products":    ["product_id"],
    "payments":    ["order_id", "payment_sequential"],
    "reviews":     ["review_id"],
    "geo":         ["geolocation_zip_code_prefix"],
    "translation": ["product_category_name"]
}

explicit_dfs = {
    "orders":      df_orders,
    "items":       df_items,
    "customers":   df_customers,
    "sellers":     df_sellers,
    "products":    df_products,
    "payments":    df_payments,
    "reviews":     df_reviews,
    "geo":         df_geo,
    "translation": df_translation
}

from pyspark.sql import Row

all_summary_rows = []

for table_name, df in explicit_dfs.items():
    total_rows = df.count()
    pk_cols = primary_keys[table_name]

    # Duplicate count on primary key
    dup_count = (
        df.groupBy(pk_cols)
          .count()
          .filter(col("count") > 1)
          .count()
    )

    for c in df.columns:
        dtype = df.schema[c].dataType

        # Null count
        null_count = df.filter(col(c).isNull()).count()

        # Min / Max for numeric and timestamp
        if isinstance(dtype, (NumericType, TSType)):
            min_val = str(df.agg(spark_min(col(c))).collect()[0][0])
            max_val = str(df.agg(spark_max(col(c))).collect()[0][0])
            top_val = "N/A"
        else:
            # Most frequent value for string columns
            top_row = (
                df.filter(col(c).isNotNull())
                  .groupBy(c)
                  .count()
                  .orderBy(desc("count"))
                  .limit(1)
                  .collect()
            )
            top_val = str(top_row[0][0]) if top_row else "N/A"
            min_val = "N/A"
            max_val = "N/A"

        all_summary_rows.append(Row(
            table_name      = table_name,
            column_name     = c,
            data_type       = str(dtype),
            total_rows      = total_rows,
            null_count      = null_count,
            null_pct        = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0,
            pk_duplicate_count = dup_count,
            min_value       = min_val,
            max_value       = max_val,
            top_value       = top_val
        ))

df_summary = spark.createDataFrame(all_summary_rows)
df_summary.createOrReplaceTempView("exploratory_summary")

print("Exploratory Summary Table:")
display(df_summary)

# COMMAND ----------

# A1.5 - Exploratory Summary Table (SQL)

display(spark.sql("""
    SELECT
        table_name,
        column_name,
        data_type,
        total_rows,
        null_count,
        null_pct,
        pk_duplicate_count,
        min_value,
        max_value,
        top_value
    FROM exploratory_summary
    ORDER BY table_name, null_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A2: Data Cleansing & Validation

# COMMAND ----------

# A2.1 - Missing Value Identification & Handling

from pyspark.sql.functions import col, when, count, to_timestamp, upper, trim, lit, coalesce

# Identify all columns with null values across all dataframes

tables = {
    "orders": df_orders,
    "items": df_items,
    "customers": df_customers,
    "sellers": df_sellers,
    "products": df_products,
    "payments": df_payments,
    "reviews": df_reviews,
    "geo": df_geo,
    "translation": df_translation
}

null_report_rows = []

for table_name, df in tables.items():
    total = df.count()
    for c in df.columns:
        null_cnt = df.filter(col(c).isNull()).count()
        if null_cnt > 0:
            null_report_rows.append((
                table_name,
                c,
                total,
                null_cnt,
                round(null_cnt / total * 100, 2)
            ))

df_null_report = spark.createDataFrame(
    null_report_rows,
    ["table_name", "column_name", "total_rows", "null_count", "null_pct"]
)

df_null_report.createOrReplaceTempView("null_report")
display(df_null_report.orderBy("table_name", "null_pct"))

# COMMAND ----------

# A2.1 - Missing Value Identification & Handling (SQL)

display(spark.sql("""
    SELECT
        table_name,
        column_name,
        total_rows,
        null_count,
        null_pct,
        CASE
            WHEN null_pct > 50 THEN 'drop column'
            WHEN column_name IN ('order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date') THEN 'flag'
            WHEN column_name IN ('product_category_name', 'review_comment_title', 'review_comment_message') THEN 'impute with unknown/empty'
            ELSE 'flag'
        END AS recommended_action
    FROM null_report
    ORDER BY table_name, null_pct DESC
"""))

# COMMAND ----------

# A2.1 - Apply missing value handling based on documented decisions

# Orders: flag rows where delivery-related dates are null
df_orders_clean = df_orders.withColumn(
    "has_missing_dates",
    when(
        col("order_approved_at").isNull() |
        col("order_delivered_carrier_date").isNull() |
        col("order_delivered_customer_date").isNull(),
        True
    ).otherwise(False)
).withColumn(
    # Impute approved_at with purchase timestamp when null (reasonable assumption)
    "order_approved_at",
    when(col("order_approved_at").isNull(), col("order_purchase_timestamp"))
    .otherwise(col("order_approved_at"))
)

# Products: impute null category name with 'unknown'
df_products_clean = df_products.withColumn(
    "product_category_name",
    when(col("product_category_name").isNull(), lit("unknown"))
    .otherwise(col("product_category_name"))
)

# Reviews: impute null comment fields with empty string
df_reviews_clean = df_reviews.withColumn(
    "review_comment_title",
    when(col("review_comment_title").isNull(), lit(""))
    .otherwise(col("review_comment_title"))
).withColumn(
    "review_comment_message",
    when(col("review_comment_message").isNull(), lit(""))
    .otherwise(col("review_comment_message"))
)

# Customers, sellers, geo, items, payments: no critical nulls, keep as-is
df_customers_clean = df_customers
df_sellers_clean = df_sellers
df_geo_clean = df_geo
df_items_clean = df_items
df_payments_clean = df_payments
df_translation_clean = df_translation

print("Missing value handling applied.")

# COMMAND ----------

# A2.2 - Duplicate Detection & Resolution

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

primary_keys = {
    "orders": ["order_id"],
    "items": ["order_id", "order_item_id"],
    "customers": ["customer_id"],
    "sellers": ["seller_id"],
    "products": ["product_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id"],
    "geo": ["geolocation_zip_code_prefix"],
    "translation": ["product_category_name"]
}

clean_dfs = {
    "orders": df_orders_clean,
    "items": df_items_clean,
    "customers": df_customers_clean,
    "sellers": df_sellers_clean,
    "products": df_products_clean,
    "payments": df_payments_clean,
    "reviews": df_reviews_clean,
    "geo": df_geo_clean,
    "translation": df_translation_clean
}

dup_report_rows = []

deduped_dfs = {}

for table_name, df in clean_dfs.items():
    before_count = df.count()
    pk = primary_keys[table_name]

    # Check full row duplicates
    full_dup_count = before_count - df.dropDuplicates().count()

    # Check primary key duplicates
    pk_dup_count = (
        df.groupBy(pk)
          .count()
          .filter(col("count") > 1)
          .count()
    )

    # Remove full row duplicates first
    df_no_full_dups = df.dropDuplicates()

    # Remove primary key duplicates by keeping first occurrence
    window_spec = Window.partitionBy(pk).orderBy(pk[0])
    df_deduped = (
        df_no_full_dups
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    after_count = df_deduped.count()
    deduped_dfs[table_name] = df_deduped

    dup_report_rows.append((
        table_name,
        before_count,
        full_dup_count,
        pk_dup_count,
        after_count,
        before_count - after_count
    ))

df_dup_report = spark.createDataFrame(
    dup_report_rows,
    ["table_name", "before_count", "full_row_duplicates",
     "pk_duplicates", "after_count", "rows_removed"]
)

df_dup_report.createOrReplaceTempView("dup_report")
display(df_dup_report)

# COMMAND ----------

# A2.2 - Duplicate Detection & Resolution (SQL)

display(spark.sql("""
    SELECT
        table_name,
        before_count,
        full_row_duplicates,
        pk_duplicates,
        after_count,
        rows_removed,
        ROUND((rows_removed / before_count) * 100, 2) AS pct_removed
    FROM dup_report
    ORDER BY rows_removed DESC
"""))

# COMMAND ----------

# A2.3 - Data Type & Format Consistency

from pyspark.sql.functions import initcap, to_date

# Unpack deduped dataframes for clarity
df_orders_clean = deduped_dfs["orders"]
df_items_clean = deduped_dfs["items"]
df_customers_clean = deduped_dfs["customers"]
df_sellers_clean = deduped_dfs["sellers"]
df_products_clean = deduped_dfs["products"]
df_payments_clean = deduped_dfs["payments"]
df_reviews_clean = deduped_dfs["reviews"]
df_geo_clean = deduped_dfs["geo"]
df_translation_clean = deduped_dfs["translation"]

# Customers: standardize city to title case, state to uppercase, trim whitespace
df_customers_clean = (
    df_customers_clean
    .withColumn("customer_city", initcap(trim(col("customer_city"))))
    .withColumn("customer_state", upper(trim(col("customer_state"))))
)

# Sellers: standardize city to title case, state to uppercase, trim whitespace
df_sellers_clean = (
    df_sellers_clean
    .withColumn("seller_city", initcap(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
)

# Geo: standardize city to title case, state to uppercase, trim whitespace
df_geo_clean = (
    df_geo_clean
    .withColumn("geolocation_city", initcap(trim(col("geolocation_city"))))
    .withColumn("geolocation_state", upper(trim(col("geolocation_state"))))
)

# Products: trim category name, convert to lowercase for consistency
df_products_clean = df_products_clean.withColumn(
    "product_category_name",
    trim(col("product_category_name"))
)

# Reviews: trim comment fields
df_reviews_clean = (
    df_reviews_clean
    .withColumn("review_comment_title", trim(col("review_comment_title")))
    .withColumn("review_comment_message", trim(col("review_comment_message")))
)

# Orders: confirm all timestamp columns are correctly typed (already set via schema)
# Cast order_estimated_delivery_date to date only for display convenience
df_orders_clean = df_orders_clean.withColumn(
    "order_estimated_delivery_date",
    to_timestamp(col("order_estimated_delivery_date"))
)

print("Data type and format standardization applied.")

# COMMAND ----------

# A2.3 - Data Type & Format Consistency (SQL)

df_customers_clean.createOrReplaceTempView("customers_clean")
df_sellers_clean.createOrReplaceTempView("sellers_clean")
df_geo_clean.createOrReplaceTempView("geo_clean")

display(spark.sql("""
    SELECT
        customer_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix
    FROM customers_clean
    LIMIT 20
"""))

display(spark.sql("""
    SELECT
        INITCAP(TRIM(seller_city)) AS seller_city,
        UPPER(TRIM(seller_state)) AS seller_state,
        COUNT(*) AS seller_count
    FROM sellers_clean
    GROUP BY seller_city, seller_state
    ORDER BY seller_count DESC
    LIMIT 20
"""))

# COMMAND ----------

# A2.4 - Value Range & Validity Checking

# Flag invalid prices and freight values in items
df_items_clean = df_items_clean.withColumn(
    "invalid_price",
    when((col("price") <= 0) | col("price").isNull(), True).otherwise(False)
).withColumn(
    "invalid_freight",
    when((col("freight_value") < 0) | col("freight_value").isNull(), True).otherwise(False)
)

# Flag invalid payment values
df_payments_clean = df_payments_clean.withColumn(
    "invalid_payment",
    when((col("payment_value") <= 0) | col("payment_value").isNull(), True).otherwise(False)
)

# Flag invalid review scores (must be between 1 and 5)
df_reviews_clean = df_reviews_clean.withColumn(
    "invalid_score",
    when(
        col("review_score").isNull() |
        (col("review_score") < 1) |
        (col("review_score") > 5),
        True
    ).otherwise(False)
)

# Flag date anomalies in orders where purchase is after delivery
df_orders_clean = df_orders_clean.withColumn(
    "date_anomaly",
    when(
        col("order_purchase_timestamp") > col("order_delivered_customer_date"),
        True
    ).otherwise(False)
)

# Print counts of flagged records
print("Invalid prices in items: ", df_items_clean.filter(col("invalid_price") == True).count())
print("Invalid freight in items: ", df_items_clean.filter(col("invalid_freight") == True).count())
print("Invalid payments: ", df_payments_clean.filter(col("invalid_payment") == True).count())
print("Invalid review scores: ", df_reviews_clean.filter(col("invalid_score") == True).count())
print("Date anomalies in orders: ", df_orders_clean.filter(col("date_anomaly") == True).count())

# COMMAND ----------

# A2.4 - Value Range & Validity Checking (SQL)

df_items_clean.createOrReplaceTempView("items_clean")
df_payments_clean.createOrReplaceTempView("payments_clean")
df_reviews_clean.createOrReplaceTempView("reviews_clean")
df_orders_clean.createOrReplaceTempView("orders_clean")

display(spark.sql("""
    SELECT
        'items - invalid price' AS check_name, COUNT(*) AS flagged_rows FROM items_clean WHERE invalid_price = true UNION ALL
        SELECT 'items - invalid freight', COUNT(*) FROM items_clean WHERE invalid_freight = true UNION ALL
        SELECT 'payments - invalid value', COUNT(*) FROM payments_clean WHERE invalid_payment = true UNION ALL
        SELECT 'reviews - invalid score', COUNT(*) FROM reviews_clean WHERE invalid_score = true UNION ALL
        SELECT 'orders - date anomaly', COUNT(*) FROM orders_clean WHERE date_anomaly = true
"""))

# COMMAND ----------

# A2.5 - Document Cleansing Actions

from pyspark.sql import Row
from datetime import datetime

log_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

cleansing_log = [
    Row(table="orders", column="order_approved_at", issue="null values", action="imputed with order_purchase_timestamp", before=df_orders.count(), after=df_orders_clean.count(), logged_at=log_timestamp),
    Row(table="orders", column="order_delivered_carrier_date", issue="null values", action="flagged as has_missing_dates = true", before=df_orders.count(), after=df_orders_clean.count(), logged_at=log_timestamp),
    Row(table="orders", column="order_delivered_customer_date", issue="null values", action="flagged as has_missing_dates = true", before=df_orders.count(), after=df_orders_clean.count(), logged_at=log_timestamp),
    Row(table="orders", column="order_purchase_timestamp", issue="date anomaly", action="flagged as date_anomaly = true", before=df_orders.count(), after=df_orders_clean.count(), logged_at=log_timestamp),
    Row(table="products", column="product_category_name", issue="null values", action="imputed with 'unknown'", before=df_products.count(), after=df_products_clean.count(), logged_at=log_timestamp),
    Row(table="reviews",  column="review_comment_title", issue="null values", action="imputed with empty string", before=df_reviews.count(), after=df_reviews_clean.count(), logged_at=log_timestamp),
    Row(table="reviews", column="review_comment_message", issue="null values", action="imputed with empty string", before=df_reviews.count(), after=df_reviews_clean.count(), logged_at=log_timestamp),
    Row(table="reviews", column="review_score", issue="out of range (1-5)", action="flagged as invalid_score = true", before=df_reviews.count(), after=df_reviews_clean.count(), logged_at=log_timestamp),
    Row(table="items", column="price", issue="zero or negative", action="flagged as invalid_price = true", before=df_items.count(), after=df_items_clean.count(), logged_at=log_timestamp),
    Row(table="items",    column="freight_value", issue="negative value", action="flagged as invalid_freight = true", before=df_items.count(), after=df_items_clean.count(), logged_at=log_timestamp),
    Row(table="payments", column="payment_value", issue="zero or negative", action="flagged as invalid_payment = true", before=df_payments.count(), after=df_payments_clean.count(), logged_at=log_timestamp),
    Row(table="customers",column="customer_city", issue="inconsistent casing", action="standardized with initcap and trim", before=df_customers.count(), after=df_customers_clean.count(), logged_at=log_timestamp),
    Row(table="customers", column="customer_state", issue="inconsistent casing", action="standardized with upper and trim", before=df_customers.count(), after=df_customers_clean.count(), logged_at=log_timestamp),
    Row(table="sellers", column="seller_city", issue="inconsistent casing", action="standardized with initcap and trim", before=df_sellers.count(), after=df_sellers_clean.count(), logged_at=log_timestamp),
    Row(table="sellers", column="seller_state", issue="inconsistent casing", action="standardized with upper and trim", before=df_sellers.count(), after=df_sellers_clean.count(), logged_at=log_timestamp),
    Row(table="geo", column="geolocation_city", issue="inconsistent casing", action="standardized with initcap and trim", before=df_geo.count(), after=df_geo_clean.count(), logged_at=log_timestamp),
    Row(table="geo", column="geolocation_state", issue="inconsistent casing", action="standardized with upper and trim", before=df_geo.count(), after=df_geo_clean.count(), logged_at=log_timestamp),
]

df_cleansing_log = spark.createDataFrame(cleansing_log)
df_cleansing_log.createOrReplaceTempView("cleansing_log")

display(df_cleansing_log)

# COMMAND ----------

# A2.5 - Document Cleansing Actions (SQL)

display(spark.sql("""
    SELECT
        table,
        column,
        issue,
        action,
        before,
        after,
        (before - after) AS rows_removed,
        logged_at
    FROM cleansing_log
    ORDER BY table, column
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A3: Enrichment & CDC Design

# COMMAND ----------

# A3.1 - Build Geography SCD2

from pyspark.sql.functions import (
    col, lag, row_number, when, lit, monotonically_increasing_id,
    lead, coalesce, max as spark_max
)
from pyspark.sql.window import Window

# Remove exact duplicates first to reduce noise
df_geo_scd = df_geo_clean.dropDuplicates()

# Create a window partitioned by zip code prefix ordered by city and state
window_zip = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
    "geolocation_city", "geolocation_state"
)

# Assign a sequence number within each zip prefix
df_geo_scd = df_geo_scd.withColumn("seq", row_number().over(window_zip))

# Detect changes: compare current city/state with previous row within same zip
df_geo_scd = df_geo_scd.withColumn(
    "prev_city",
    lag("geolocation_city", 1).over(window_zip)
).withColumn(
    "prev_state",
    lag("geolocation_state", 1).over(window_zip)
)

# Mark a new version when city or state changes from previous row
df_geo_scd = df_geo_scd.withColumn(
    "is_new_version",
    when(
        col("prev_city").isNull() |
        (col("geolocation_city") != col("prev_city")) |
        (col("geolocation_state") != col("prev_state")),
        1
    ).otherwise(0)
)

# Assign version number using cumulative sum of is_new_version
window_zip_seq = Window.partitionBy("geolocation_zip_code_prefix").orderBy("seq").rowsBetween(Window.unboundedPreceding, 0)

df_geo_scd = df_geo_scd.withColumn(
    "version_num",
    col("is_new_version")
)

# Use running sum to get version number
from pyspark.sql.functions import sum as spark_sum

df_geo_scd = df_geo_scd.withColumn(
    "version_num",
    spark_sum("is_new_version").over(window_zip_seq)
)

# Keep one row per zip + version (deduplicate within same version)
window_version = Window.partitionBy(
    "geolocation_zip_code_prefix", "version_num"
).orderBy("seq")

df_geo_scd = df_geo_scd.withColumn(
    "row_within_version",
    row_number().over(window_version)
).filter(col("row_within_version") == 1)

# Compute effective_start and effective_end using lead
window_zip_ver = Window.partitionBy("geolocation_zip_code_prefix").orderBy("version_num")

df_geo_scd = df_geo_scd.withColumn(
    "next_version",
    lead("version_num", 1).over(window_zip_ver)
)

# Determine is_current flag
df_geo_scd = df_geo_scd.withColumn(
    "is_current",
    when(col("next_version").isNull(), True).otherwise(False)
)

# Build final dim_geography with relevant columns only
dim_geography = df_geo_scd.select(
    col("geolocation_zip_code_prefix").alias("zip_prefix"),
    col("geolocation_city").alias("city"),
    col("geolocation_state").alias("state"),
    col("geolocation_lat").alias("lat"),
    col("geolocation_lng").alias("lng"),
    col("version_num"),
    col("is_current")
)

dim_geography.createOrReplaceTempView("dim_geography")
print("dim_geography row count:", dim_geography.count())
display(dim_geography.filter(col("version_num") > 1).orderBy("zip_prefix", "version_num"))

# COMMAND ----------

# A3.1 - Build Geography SCD2 (SQL)

display(spark.sql("""
    SELECT
        zip_prefix,
        city,
        state,
        version_num,
        is_current
    FROM dim_geography
    WHERE zip_prefix IN (
        SELECT zip_prefix
        FROM dim_geography
        GROUP BY zip_prefix
        HAVING COUNT(*) > 1
    )
    ORDER BY zip_prefix, version_num
    LIMIT 50
"""))

# COMMAND ----------

# A3.2 - Validate and Repair Stage Timestamps

from pyspark.sql.functions import unix_timestamp, abs as spark_abs

# Validate non-decreasing sequence: purchase <= approved <= carrier <= delivered
df_orders_ts = df_orders_clean.withColumn(
    "ts_anomaly_approved",
    when(
        col("order_approved_at") < col("order_purchase_timestamp"),
        True
    ).otherwise(False)
).withColumn(
    "ts_anomaly_carrier",
    when(
        col("order_delivered_carrier_date") < col("order_approved_at"),
        True
    ).otherwise(False)
).withColumn(
    "ts_anomaly_delivered",
    when(
        col("order_delivered_customer_date") < col("order_delivered_carrier_date"),
        True
    ).otherwise(False)
).withColumn(
    "any_ts_anomaly",
    when(
        col("ts_anomaly_approved") |
        col("ts_anomaly_carrier") |
        col("ts_anomaly_delivered"),
        True
    ).otherwise(False)
)

# Repair: forward-fill timestamps where discrepancy is <= 2 hours (7200 seconds)
two_hours = 7200

df_orders_ts = df_orders_ts.withColumn(
    "order_approved_at",
    when(
        col("ts_anomaly_approved") &
        (spark_abs(
            unix_timestamp("order_approved_at") -
            unix_timestamp("order_purchase_timestamp")
        ) <= two_hours),
        col("order_purchase_timestamp")
    ).otherwise(col("order_approved_at"))
).withColumn(
    "order_delivered_carrier_date",
    when(
        col("ts_anomaly_carrier") &
        (spark_abs(
            unix_timestamp("order_delivered_carrier_date") -
            unix_timestamp("order_approved_at")
        ) <= two_hours),
        col("order_approved_at")
    ).otherwise(col("order_delivered_carrier_date"))
).withColumn(
    "order_delivered_customer_date",
    when(
        col("ts_anomaly_delivered") &
        (spark_abs(
            unix_timestamp("order_delivered_customer_date") -
            unix_timestamp("order_delivered_carrier_date")
        ) <= two_hours),
        col("order_delivered_carrier_date")
    ).otherwise(col("order_delivered_customer_date"))
)

df_orders_ts.createOrReplaceTempView("orders_ts")

print("Total orders:", df_orders_ts.count())
print("Approved anomalies:", df_orders_ts.filter(col("ts_anomaly_approved")).count())
print("Carrier anomalies:", df_orders_ts.filter(col("ts_anomaly_carrier")).count())
print("Delivered anomalies:", df_orders_ts.filter(col("ts_anomaly_delivered")).count())
print("Repaired rows (approved):", df_orders_ts.filter(col("ts_anomaly_approved") & ~col("any_ts_anomaly")).count())

display(df_orders_ts.filter(col("any_ts_anomaly")).select(
    "order_id",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "ts_anomaly_approved",
    "ts_anomaly_carrier",
    "ts_anomaly_delivered"
).limit(20))

# COMMAND ----------

# A3.2 - Validate and Repair Stage Timestamps (SQL)

display(spark.sql("""
    SELECT
        order_id,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        ts_anomaly_approved,
        ts_anomaly_carrier,
        ts_anomaly_delivered,
        any_ts_anomaly
    FROM orders_ts
    WHERE any_ts_anomaly = true
    LIMIT 30
"""))

# COMMAND ----------

# A3.3 - Seller Trailing 30-Day On-Time Delivery Rate

from pyspark.sql.functions import datediff, avg, expr

# Compute on-time flag at order level
df_orders_ontime = df_orders_ts.withColumn(
    "is_on_time",
    when(
        col("order_delivered_customer_date") <= col("order_estimated_delivery_date"),
        1
    ).otherwise(0)
).select(
    "order_id",
    "order_purchase_timestamp",
    "is_on_time"
)

# Join items with orders to get seller + purchase date + on_time flag
df_items_orders = df_items_clean.join(
    df_orders_ontime,
    on="order_id",
    how="left"
).select(
    col("order_id"),
    col("seller_id"),
    col("order_purchase_timestamp").alias("purchase_date"),
    col("is_on_time")
)

# Self join to compute trailing 30-day rate per seller per purchase date
# For each row, find all deliveries by same seller in the 30 days strictly before purchase date
df_left = df_items_orders.alias("curr")
df_right = df_items_orders.alias("hist")

df_trailing = df_left.join(
    df_right,
    on=(
        (col("curr.seller_id") == col("hist.seller_id")) &
        (col("hist.purchase_date") < col("curr.purchase_date")) &
        (col("hist.purchase_date") >= expr("date_sub(curr.purchase_date, 30)"))
    ),
    how="left"
).groupBy(
    col("curr.order_id"),
    col("curr.seller_id"),
    col("curr.purchase_date")
).agg(
    avg(col("hist.is_on_time")).alias("trailing_30d_ontime_rate")
)

# Join rate back to items
df_items_enriched = df_items_clean.join(
    df_trailing.select("order_id", "seller_id", "trailing_30d_ontime_rate"),
    on=["order_id", "seller_id"],
    how="left"
)

df_items_enriched.createOrReplaceTempView("items_enriched")

print("Items enriched row count:", df_items_enriched.count())
display(df_items_enriched.select(
    "order_id", "seller_id", "price", "freight_value", "trailing_30d_ontime_rate"
).limit(20))

# COMMAND ----------

# A3.3 - Seller Trailing 30-Day On-Time Delivery Rate (SQL)

display(spark.sql("""
    SELECT
        order_id,
        seller_id,
        price,
        freight_value,
        ROUND(trailing_30d_ontime_rate, 4) AS trailing_30d_ontime_rate
    FROM items_enriched
    WHERE trailing_30d_ontime_rate IS NOT NULL
    ORDER BY trailing_30d_ontime_rate ASC
    LIMIT 30
"""))

# COMMAND ----------

# A3.4 - Funnel Conversion Rates Per Seller Per Month

from pyspark.sql.functions import date_trunc, countDistinct, round as spark_round

df_funnel_base = df_orders_ts.join(
    df_items_clean.select("order_id", "seller_id").dropDuplicates(["order_id", "seller_id"]),
    on="order_id",
    how="inner"
)

# Extract month from purchase timestamp
df_funnel_base = df_funnel_base.withColumn(
    "order_month",
    date_trunc("month", col("order_purchase_timestamp"))
)

window_seller_month = Window.partitionBy("seller_id", "order_month")

# Compute funnel counts using distinct order counts per seller per month
df_funnel = df_funnel_base.groupBy("seller_id", "order_month").agg(
    countDistinct("order_id").alias("purchased_count"),
    countDistinct(
        when(col("order_approved_at").isNotNull(), col("order_id"))
    ).alias("approved_count"),
    countDistinct(
        when(col("order_delivered_customer_date").isNotNull(), col("order_id"))
    ).alias("delivered_count")
)

# Compute conversion rates
df_funnel = df_funnel.withColumn(
    "p_to_a_rate",
    spark_round(
        when(col("purchased_count") > 0,
            col("approved_count") / col("purchased_count")
        ).otherwise(None),
        4
    )
).withColumn(
    "a_to_d_rate",
    spark_round(
        when(col("approved_count") > 0,
            col("delivered_count") / col("approved_count")
        ).otherwise(None),
        4
    )
)

df_funnel.createOrReplaceTempView("seller_funnel")

print("Seller funnel row count:", df_funnel.count())
display(df_funnel.orderBy("seller_id", "order_month").limit(30))

# COMMAND ----------

# A3.4 - Funnel Conversion Rates Per Seller Per Month (SQL)

display(spark.sql("""
    SELECT
        seller_id,
        order_month,
        purchased_count,
        approved_count,
        delivered_count,
        p_to_a_rate,
        a_to_d_rate
    FROM seller_funnel
    ORDER BY order_month DESC, p_to_a_rate ASC
    LIMIT 30
"""))

# COMMAND ----------

# A3.5 - Revenue Share by Category Per Seller (Top 5 + Others)

from pyspark.sql.functions import sum as spark_sum, round as spark_round

df_items_cat = df_items_clean.join(
    df_products_clean.select("product_id", "product_category_name"),
    on="product_id",
    how="left"
)

df_seller_cat_rev = df_items_cat.groupBy("seller_id", "product_category_name").agg(
    spark_sum("price").alias("category_revenue")
)

# Rank categories per seller by revenue descending
window_seller = Window.partitionBy("seller_id").orderBy(col("category_revenue").desc())

df_seller_cat_rev = df_seller_cat_rev.withColumn(
    "rank",
    row_number().over(window_seller)
)

df_seller_total = df_seller_cat_rev.groupBy("seller_id").agg(
    spark_sum("category_revenue").alias("total_revenue")
)

# Join total revenue back
df_seller_cat_rev = df_seller_cat_rev.join(df_seller_total, on="seller_id", how="left")

# Label top 5 as-is, rest as OTHERS
df_seller_cat_rev = df_seller_cat_rev.withColumn(
    "category_label",
    when(col("rank") <= 5, col("product_category_name")).otherwise(lit("OTHERS"))
)

# Aggregate OTHERS into one row per seller
df_revenue_share = df_seller_cat_rev.groupBy("seller_id", "category_label", "total_revenue").agg(
    spark_sum("category_revenue").alias("category_revenue")
).withColumn(
    "revenue_share_pct",
    spark_round((col("category_revenue") / col("total_revenue")) * 100, 2)
)

df_revenue_share.createOrReplaceTempView("seller_revenue_share")

print("Seller revenue share row count:", df_revenue_share.count())
display(df_revenue_share.orderBy("seller_id", col("revenue_share_pct").desc()).limit(30))

# COMMAND ----------

# A3.5 - Revenue Share by Category Per Seller (SQL)

display(spark.sql("""
    SELECT
        seller_id,
        category_label,
        ROUND(category_revenue, 2) AS category_revenue,
        total_revenue,
        revenue_share_pct
    FROM seller_revenue_share
    ORDER BY seller_id, revenue_share_pct DESC
    LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A4: Business & Advanced Feature Aggregates

# COMMAND ----------

# A4.1 - Late-severity UDF and distribution per seller

from pyspark.sql.functions import udf, datediff, count, col, when
from pyspark.sql.types import IntegerType

# udf that buckets delivery delay into severity codes
@udf(IntegerType())
def late_severity(delivery_date, estimated_date):
    if delivery_date is None or estimated_date is None:
        return None
    delay_days = (delivery_date - estimated_date).days
    if delay_days < -1:
        return -2
    elif delay_days <= 0:
        return -1
    elif delay_days <= 2:
        return 0
    elif delay_days <= 7:
        return 1
    else:
        return 2

# join orders with items to get seller_id, filter to delivered only
df_delivery = (df_orders_ts
    .filter(col("order_status") == "delivered")
    .filter(col("order_delivered_customer_date").isNotNull())
    .filter(col("order_estimated_delivery_date").isNotNull())
    .join(df_items_clean.select("order_id", "seller_id"), on="order_id", how="inner")
)

df_severity = df_delivery.withColumn(
    "late_severity",
    late_severity("order_delivered_customer_date", "order_estimated_delivery_date")
)

df_severity.createOrReplaceTempView("delivery_severity")

# distribution per seller
df_severity_dist = (df_severity
    .groupBy("seller_id", "late_severity")
    .agg(count("*").alias("cnt"))
    .orderBy("seller_id", "late_severity")
)

df_severity_dist.createOrReplaceTempView("seller_severity_dist")
display(df_severity_dist)

# COMMAND ----------

# A4.1 - Late-severity UDF and distribution per seller (SQL)

display(spark.sql("""
    SELECT
        seller_id,
        CASE
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) < -1 THEN -2
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 0 THEN -1
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 2 THEN 0
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 7 THEN 1
            ELSE 2
        END AS late_severity,
        count(*) AS cnt
    FROM delivery_severity
    GROUP BY seller_id,
        CASE
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) < -1 THEN -2
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 0 THEN -1
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 2 THEN 0
            WHEN datediff(order_delivered_customer_date, order_estimated_delivery_date) <= 7 THEN 1
            ELSE 2
        END
    ORDER BY seller_id, late_severity
"""))

# COMMAND ----------

# A4.2 - Orders status pivot with monthly counts per state

from pyspark.sql.functions import date_format

df_orders_state = (df_orders_ts
    .join(df_customers_clean.select("customer_id", "customer_state"), on="customer_id", how="inner")
    .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM"))
)

df_orders_state.createOrReplaceTempView("orders_state")

# pivot monthly order counts by order_status for each customer_state
df_pivot_status = (df_orders_state
    .groupBy("customer_state", "order_month")
    .pivot("order_status")
    .agg(count("order_id"))
    .fillna(0)
    .orderBy("customer_state", "order_month")
)

display(df_pivot_status)

# COMMAND ----------

# A4.2 - Orders status pivot with monthly counts per state (SQL)

display(spark.sql("""
    SELECT *
    FROM (
        SELECT
            c.customer_state,
            date_format(o.order_purchase_timestamp, 'yyyy-MM') AS order_month,
            o.order_status,
            o.order_id
        FROM orders_ts o
        JOIN customers c ON o.customer_id = c.customer_id
    )
    PIVOT (
        count(order_id)
        FOR order_status IN ('delivered', 'shipped', 'canceled', 'unavailable', 'invoiced', 'processing', 'created', 'approved')
    )
    ORDER BY customer_state, order_month
"""))

# COMMAND ----------

# A4.3 - Per-category delivery delay distribution

from pyspark.sql.functions import percentile_approx, coalesce

df_delay_cat = (df_severity
    .join(df_items_clean.select("order_id", "product_id"), on="order_id", how="inner")
    .join(df_products_clean.select("product_id", "product_category_name"), on="product_id", how="inner")
    .join(df_translation_clean, on="product_category_name", how="left")
    .withColumn("category", coalesce(col("product_category_name_english"), col("product_category_name")))
    .withColumn("delay_days", datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
    .filter(col("delay_days").isNotNull())
)

df_delay_percentiles = (df_delay_cat
    .groupBy("category")
    .agg(
        percentile_approx("delay_days", 0.50).alias("p50_delay"),
        percentile_approx("delay_days", 0.90).alias("p90_delay"),
        percentile_approx("delay_days", 0.99).alias("p99_delay")
    )
    .orderBy("category")
)

df_delay_percentiles.createOrReplaceTempView("category_delay_dist")
display(df_delay_percentiles)

# COMMAND ----------

# A4.3 - Per-category delivery delay distribution (SQL)

display(spark.sql("""
    SELECT
        COALESCE(t.product_category_name_english, p.product_category_name) AS category,
        percentile_approx(datediff(ds.order_delivered_customer_date, ds.order_estimated_delivery_date), 0.50) AS p50_delay,
        percentile_approx(datediff(ds.order_delivered_customer_date, ds.order_estimated_delivery_date), 0.90) AS p90_delay,
        percentile_approx(datediff(ds.order_delivered_customer_date, ds.order_estimated_delivery_date), 0.99) AS p99_delay
    FROM delivery_severity ds
    JOIN items i ON ds.order_id = i.order_id
    JOIN products p ON i.product_id = p.product_id
    LEFT JOIN translation t ON p.product_category_name = t.product_category_name
    WHERE ds.order_delivered_customer_date IS NOT NULL
        AND ds.order_estimated_delivery_date IS NOT NULL
    GROUP BY COALESCE(t.product_category_name_english, p.product_category_name)
    ORDER BY category
"""))

# COMMAND ----------

# A4.4 - 7-day rolling on-time rate per seller

from pyspark.sql.functions import avg
from pyspark.sql.window import Window

# on_time flag already exists in df_delivery from A4.1 setup
df_ontime_base = df_severity.select(
    "order_id", "seller_id", "order_purchase_timestamp",
    when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), 1).otherwise(0).alias("on_time")
)

# 7-day window in seconds
w_7d = (Window
    .partitionBy("seller_id")
    .orderBy(col("order_purchase_timestamp").cast("long"))
    .rangeBetween(-7 * 86400, 0)
)

df_rolling_ontime = (df_ontime_base
    .withColumn("rolling_7d_ontime_rate", avg("on_time").over(w_7d))
    .orderBy("seller_id", "order_purchase_timestamp")
)

df_rolling_ontime.createOrReplaceTempView("seller_rolling_ontime")
display(df_rolling_ontime)

# COMMAND ----------

# A4.4 - 7-day rolling on-time rate per seller (SQL)

display(spark.sql("""
    SELECT
        seller_id,
        order_id,
        order_purchase_timestamp,
        CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END AS on_time,
        AVG(CASE WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 1 ELSE 0 END) OVER (
            PARTITION BY seller_id
            ORDER BY CAST(order_purchase_timestamp AS LONG)
            RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_ontime_rate
    FROM delivery_severity
    ORDER BY seller_id, order_purchase_timestamp
"""))

# COMMAND ----------

# A4.5 - Pivot payments per order into columns

# pivot payment_value by payment_type to columns with totals
from pyspark.sql.functions import sum as spark_sum

df_pay_pivot = (df_payments_clean
    .groupBy("order_id")
    .pivot("payment_type", ["credit_card", "boleto", "voucher", "debit_card"])
    .agg(spark_sum("payment_value"))
    .fillna(0)
    .withColumn("total", col("credit_card") + col("boleto") + col("voucher") + col("debit_card"))
)

df_pay_pivot.createOrReplaceTempView("payments_pivot")
display(df_pay_pivot)

# COMMAND ----------

# A4.5 - Pivot payments per order into columns (SQL)

display(spark.sql("""
    SELECT
        order_id,
        COALESCE(credit_card, 0) AS credit_card,
        COALESCE(boleto, 0) AS boleto,
        COALESCE(voucher, 0) AS voucher,
        COALESCE(debit_card, 0) AS debit_card,
        COALESCE(credit_card, 0) + COALESCE(boleto, 0) + COALESCE(voucher, 0) + COALESCE(debit_card, 0) AS total
    FROM (
        SELECT order_id, payment_type, payment_value
        FROM payments
    )
    PIVOT (
        SUM(payment_value)
        FOR payment_type IN ('credit_card' AS credit_card, 'boleto' AS boleto, 'voucher' AS voucher, 'debit_card' AS debit_card)
    )
    ORDER BY order_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A5: Performance Optimization

# COMMAND ----------

# A5.1 - Broadcast join demonstration

from pyspark.sql.functions import broadcast

# without explicit broadcast - spark may or may not auto-broadcast
df_no_broadcast = (df_items_clean
    .join(df_products_clean, on="product_id", how="inner")
    .join(df_translation_clean, on="product_category_name", how="left")
)

# with explicit broadcast on the small translation table
df_with_broadcast = (df_items_clean
    .join(df_products_clean, on="product_id", how="inner")
    .join(broadcast(df_translation_clean), on="product_category_name", how="left")
)

print("without explicit broadcast:")
df_no_broadcast.explain()

print("with explicit broadcast:")
df_with_broadcast.explain()

display(df_with_broadcast.limit(10))

# COMMAND ----------

# A5.1 - Broadcast join demonstration (SQL)

display(spark.sql("""
    SELECT /*+ BROADCAST(t) */
        i.order_id,
        i.product_id,
        p.product_category_name,
        t.product_category_name_english
    FROM items i
    JOIN products p ON i.product_id = p.product_id
    LEFT JOIN translation t ON p.product_category_name = t.product_category_name
    LIMIT 10
"""))

# COMMAND ----------

# A5.2 - Z-ORDER and partitioning demonstration

# partitioning: physically splits data into folders by column values
df_orders_ts.write.format("delta").mode("overwrite").partitionBy("order_status").saveAsTable("main.default.orders_partitioned")

df_partitioned = spark.read.table("main.default.orders_partitioned")
print("partitioned row count:", df_partitioned.count())

# z-order: reorders data within delta files for co-locality
df_orders_ts.write.format("delta").mode("overwrite").saveAsTable("main.default.orders_delta_zorder")
spark.sql("OPTIMIZE main.default.orders_delta_zorder ZORDER BY (customer_id)")

print("z-order optimization complete on customer_id")

# COMMAND ----------

# A5.2 - Z-ORDER and partitioning demonstration (SQL)

display(spark.sql("""
    DESCRIBE DETAIL main.default.orders_delta_zorder
"""))

# COMMAND ----------

# A5.3 - Approximate sessions per customer

from pyspark.sql.functions import lag, datediff, sum as spark_sum, col, when, avg, count
from pyspark.sql.window import Window

# window ordered by purchase timestamp per customer
w_cust = Window.partitionBy("customer_id").orderBy("order_purchase_timestamp")

# compute gap in days between consecutive orders per customer
df_sessions = (df_orders_ts
    .select("order_id", "customer_id", "order_purchase_timestamp")
    .withColumn("prev_purchase", lag("order_purchase_timestamp", 1).over(w_cust))
    .withColumn("gap_days", datediff("order_purchase_timestamp", "prev_purchase"))
    .withColumn("new_session", when((col("gap_days") > 7) | col("gap_days").isNull(), 1).otherwise(0))
)

# assign session_id using cumulative sum of new_session flags
w_cust_ordered = Window.partitionBy("customer_id").orderBy("order_purchase_timestamp").rowsBetween(Window.unboundedPreceding, 0)

df_sessions = df_sessions.withColumn("session_id", spark_sum("new_session").over(w_cust_ordered))

# compute session count and average orders per session per customer
df_session_stats = (df_sessions
    .groupBy("customer_id", "session_id")
    .agg(count("order_id").alias("orders_in_session"))
)

df_customer_sessions = (df_session_stats
    .groupBy("customer_id")
    .agg(
        count("session_id").alias("session_count"),
        avg("orders_in_session").alias("avg_orders_per_session")
    )
    .orderBy(col("session_count").desc())
)

df_customer_sessions.createOrReplaceTempView("customer_sessions")
display(df_customer_sessions)

# COMMAND ----------

# A5.3 - Approximate sessions per customer (SQL)

display(spark.sql("""
    WITH ordered_orders AS (
        SELECT
            order_id,
            customer_id,
            order_purchase_timestamp,
            LAG(order_purchase_timestamp, 1) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp) AS prev_purchase
        FROM orders_ts
    ),
    with_gaps AS (
        SELECT *,
            datediff(order_purchase_timestamp, prev_purchase) AS gap_days,
            CASE WHEN datediff(order_purchase_timestamp, prev_purchase) > 7 OR prev_purchase IS NULL THEN 1 ELSE 0 END AS new_session
        FROM ordered_orders
    ),
    with_sessions AS (
        SELECT *,
            SUM(new_session) OVER (PARTITION BY customer_id ORDER BY order_purchase_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
        FROM with_gaps
    ),
    session_sizes AS (
        SELECT customer_id, session_id, COUNT(order_id) AS orders_in_session
        FROM with_sessions
        GROUP BY customer_id, session_id
    )
    SELECT
        customer_id,
        COUNT(session_id) AS session_count,
        AVG(orders_in_session) AS avg_orders_per_session
    FROM session_sizes
    GROUP BY customer_id
    ORDER BY session_count DESC
"""))

# COMMAND ----------

# A5.4 - Average review score by state-month

from pyspark.sql.functions import row_number, date_format, round as spark_round

# window to pick most recent review per order
w_review = Window.partitionBy("order_id").orderBy(col("review_answer_timestamp").desc())

# keep only the latest review per order
df_latest_review = (df_reviews_clean
    .withColumn("rn", row_number().over(w_review))
    .filter(col("rn") == 1)
    .drop("rn")
)

# join with orders and customers to get state and month
df_review_state = (df_latest_review
    .join(df_orders_ts.select("order_id", "customer_id", "order_purchase_timestamp"), on="order_id", how="inner")
    .join(df_customers_clean.select("customer_id", "customer_state"), on="customer_id", how="inner")
    .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM"))
)

# compute average score by state-month
df_avg_score = (df_review_state
    .groupBy("customer_state", "order_month")
    .agg(spark_round(avg("review_score"), 2).alias("avg_review_score"))
    .orderBy("customer_state", "order_month")
)

df_avg_score.createOrReplaceTempView("avg_score_state_month")
display(df_avg_score)

# COMMAND ----------

# A5.4 - Average review score by state-month (SQL)

display(spark.sql("""
    WITH latest_reviews AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_answer_timestamp DESC) AS rn
        FROM reviews
    )
    SELECT
        c.customer_state,
        date_format(o.order_purchase_timestamp, 'yyyy-MM') AS order_month,
        ROUND(AVG(r.review_score), 2) AS avg_review_score
    FROM latest_reviews r
    JOIN orders_ts o ON r.order_id = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE r.rn = 1
    GROUP BY c.customer_state, date_format(o.order_purchase_timestamp, 'yyyy-MM')
    ORDER BY customer_state, order_month
"""))

# COMMAND ----------

# A5.5 - Flag freight outliers per seller

from pyspark.sql.functions import percentile_approx, col

# compute p95 freight per seller
df_seller_p95 = (df_items_clean
    .groupBy("seller_id")
    .agg(percentile_approx("freight_value", 0.95).alias("p95_freight"))
)

# join back and flag outliers
df_freight_flagged = (df_items_clean
    .join(df_seller_p95, on="seller_id", how="left")
    .withColumn("freight_outlier", when(col("freight_value") > col("p95_freight"), True).otherwise(False))
)

df_freight_flagged.createOrReplaceTempView("freight_outliers")

print("total items:", df_freight_flagged.count())
print("freight outliers:", df_freight_flagged.filter(col("freight_outlier")).count())

display(df_freight_flagged.filter(col("freight_outlier")).select(
    "order_id", "seller_id", "freight_value", "p95_freight", "freight_outlier"
))

# COMMAND ----------

# A5.5 - Flag freight outliers per seller (SQL)

display(spark.sql("""
    WITH seller_p95 AS (
        SELECT seller_id, percentile_approx(freight_value, 0.95) AS p95_freight
        FROM items
        GROUP BY seller_id
    )
    SELECT
        i.order_id,
        i.seller_id,
        i.freight_value,
        s.p95_freight,
        CASE WHEN i.freight_value > s.p95_freight THEN true ELSE false END AS freight_outlier
    FROM items i
    JOIN seller_p95 s ON i.seller_id = s.seller_id
    WHERE i.freight_value > s.p95_freight
    ORDER BY i.freight_value DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A6: Data Visualization & Dashboarding

# COMMAND ----------

# A6.1 - Setup base data for dashboarding

from pyspark.sql.functions import col, sum as spark_sum, round as spark_round, datediff, date_format, coalesce, countDistinct

# build a unified revenue table joining orders, items, customers, payments, products, and categories
df_revenue_base = (df_items_clean
    .join(df_orders_ts.select("order_id", "customer_id", "order_purchase_timestamp", "order_status",
        "order_delivered_customer_date", "order_estimated_delivery_date"), on="order_id", how="inner")
    .join(df_customers_clean.select("customer_id", "customer_state"), on="customer_id", how="inner")
    .join(df_products_clean.select("product_id", "product_category_name"), on="product_id", how="left")
    .join(df_translation_clean, on="product_category_name", how="left")
    .withColumn("category", coalesce(col("product_category_name_english"), col("product_category_name")))
    .withColumn("total_revenue", col("price") + col("freight_value"))
    .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM"))
    .withColumn("delay_days", datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
)

df_revenue_base.createOrReplaceTempView("revenue_base")
print("revenue base row count:", df_revenue_base.count())

# COMMAND ----------

# A6.1 - KPI card: overall total revenue

df_kpi_total = (df_revenue_base
    .agg(
        spark_round(spark_sum("total_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers")
    )
)

display(df_kpi_total)

# COMMAND ----------

# A6.1 - KPI card: overall total revenue (SQL)

display(spark.sql("""
    SELECT
        ROUND(SUM(total_revenue), 2) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM revenue_base
"""))

# COMMAND ----------

# A6.2 - Bar chart: top 10 categories by revenue

from pyspark.sql.functions import sum as spark_sum, round as spark_round, col

df_top_categories = (df_revenue_base
    .filter(col("category").isNotNull())
    .groupBy("category")
    .agg(spark_round(spark_sum("total_revenue"), 2).alias("category_revenue"))
    .orderBy(col("category_revenue").desc())
    .limit(10)
)

display(df_top_categories)

# COMMAND ----------

# A6.2 - Bar chart: top 10 categories by revenue (SQL)

display(spark.sql("""
    SELECT
        category,
        ROUND(SUM(total_revenue), 2) AS category_revenue
    FROM revenue_base
    WHERE category IS NOT NULL
    GROUP BY category
    ORDER BY category_revenue DESC
    LIMIT 10
"""))

# COMMAND ----------

# A6.3 - Line chart: revenue by month

df_monthly_revenue = (df_revenue_base
    .groupBy("order_month")
    .agg(spark_round(spark_sum("total_revenue"), 2).alias("monthly_revenue"))
    .orderBy("order_month")
)

display(df_monthly_revenue)

# COMMAND ----------

# A6.3 - Line chart: revenue by month (SQL)

display(spark.sql("""
    SELECT
        order_month,
        ROUND(SUM(total_revenue), 2) AS monthly_revenue
    FROM revenue_base
    GROUP BY order_month
    ORDER BY order_month
"""))

# COMMAND ----------

# A6.4 - Pie chart: payment method share

df_payment_share = (df_payments_clean
    .groupBy("payment_type")
    .agg(spark_round(spark_sum("payment_value"), 2).alias("total_payment"))
    .orderBy(col("total_payment").desc())
)

display(df_payment_share)

# COMMAND ----------

# A6.4 - Pie chart: payment method share (SQL)

display(spark.sql("""
    SELECT
        payment_type,
        ROUND(SUM(payment_value), 2) AS total_payment
    FROM payments
    GROUP BY payment_type
    ORDER BY total_payment DESC
"""))

# COMMAND ----------

# A6.5 - Box plot: delivery delay distribution per category

df_delay_box = (df_revenue_base
    .filter(col("delay_days").isNotNull())
    .filter(col("category").isNotNull())
    .select("category", "delay_days")
    .orderBy("category")
)

display(df_delay_box)

# COMMAND ----------

# A6.5 - Box plot: delivery delay distribution per category (SQL)

display(spark.sql("""
    SELECT
        category,
        delay_days
    FROM revenue_base
    WHERE delay_days IS NOT NULL
        AND category IS NOT NULL
    ORDER BY category
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A7: Data Governance & Security Plan

# COMMAND ----------

# A7.1 - Access controls and RBAC plan

# define roles and their access levels
rbac_roles = [
    ("data_engineer", "bronze", "read/write", "full access to ingest and transform raw data"),
    ("data_engineer", "silver", "read/write", "clean, deduplicate, and standardize data"),
    ("data_engineer", "gold", "read/write", "create aggregates and feature tables"),
    ("data_analyst", "bronze", "no access", "analysts should not touch raw data"),
    ("data_analyst", "silver", "read only", "query cleaned data for exploration"),
    ("data_analyst", "gold", "read only", "consume dashboards and aggregated tables"),
    ("data_scientist", "bronze", "no access", "no need for raw data"),
    ("data_scientist", "silver", "read only", "feature engineering from cleaned data"),
    ("data_scientist", "gold", "read only", "model training on aggregated features"),
    ("business_user", "bronze", "no access", "no access to any raw or intermediate layers"),
    ("business_user", "silver", "no access", "no access to any raw or intermediate layers"),
    ("business_user", "gold", "read only", "view dashboards and kpi reports only"),
    ("admin", "bronze", "full control", "manage schemas, users, and permissions"),
    ("admin", "silver", "full control", "manage schemas, users, and permissions"),
    ("admin", "gold", "full control", "manage schemas, users, and permissions"),
]

df_rbac = spark.createDataFrame(rbac_roles, ["role", "layer", "access_level", "description"])
df_rbac.createOrReplaceTempView("rbac_plan")
display(df_rbac)

# COMMAND ----------

# A7.1 - Access controls and RBAC plan (SQL)

display(spark.sql("""
    SELECT role, layer, access_level, description
    FROM rbac_plan
    ORDER BY role, layer
"""))

# COMMAND ----------

# A7.2 - Data dictionary for all tables and columns

from pyspark.sql import Row

data_dict_rows = [
    # orders
    Row(table="orders", column="order_id", data_type="string", description="unique identifier for each order", pii="no", nullable="no"),
    Row(table="orders", column="customer_id", data_type="string", description="foreign key linking to customers table", pii="no", nullable="no"),
    Row(table="orders", column="order_status", data_type="string", description="current status of order (delivered, shipped, canceled, etc.)", pii="no", nullable="no"),
    Row(table="orders", column="order_purchase_timestamp", data_type="timestamp", description="timestamp when the order was placed", pii="no", nullable="no"),
    Row(table="orders", column="order_approved_at", data_type="timestamp", description="timestamp when payment was approved", pii="no", nullable="yes"),
    Row(table="orders", column="order_delivered_carrier_date", data_type="timestamp", description="timestamp when order was handed to carrier", pii="no", nullable="yes"),
    Row(table="orders", column="order_delivered_customer_date", data_type="timestamp", description="timestamp when customer received the order", pii="no", nullable="yes"),
    Row(table="orders", column="order_estimated_delivery_date", data_type="timestamp", description="estimated delivery date shown to customer at purchase", pii="no", nullable="no"),
    # customers
    Row(table="customers", column="customer_id", data_type="string", description="unique customer id per order (not globally unique)", pii="no", nullable="no"),
    Row(table="customers", column="customer_unique_id", data_type="string", description="globally unique customer identifier across orders", pii="yes", nullable="no"),
    Row(table="customers", column="customer_zip_code_prefix", data_type="string", description="first 5 digits of customer zip code", pii="yes", nullable="no"),
    Row(table="customers", column="customer_city", data_type="string", description="customer city name", pii="no", nullable="no"),
    Row(table="customers", column="customer_state", data_type="string", description="customer state abbreviation (2 letters)", pii="no", nullable="no"),
    # order items
    Row(table="items", column="order_id", data_type="string", description="foreign key to orders table", pii="no", nullable="no"),
    Row(table="items", column="order_item_id", data_type="integer", description="sequential item number within an order", pii="no", nullable="no"),
    Row(table="items", column="product_id", data_type="string", description="foreign key to products table", pii="no", nullable="no"),
    Row(table="items", column="seller_id", data_type="string", description="foreign key to sellers table", pii="no", nullable="no"),
    Row(table="items", column="shipping_limit_date", data_type="timestamp", description="seller shipping deadline for the item", pii="no", nullable="no"),
    Row(table="items", column="price", data_type="double", description="item price in brazilian real", pii="no", nullable="no"),
    Row(table="items", column="freight_value", data_type="double", description="freight cost for the item", pii="no", nullable="no"),
    # payments
    Row(table="payments", column="order_id", data_type="string", description="foreign key to orders table", pii="no", nullable="no"),
    Row(table="payments", column="payment_sequential", data_type="integer", description="sequence of payment methods for an order", pii="no", nullable="no"),
    Row(table="payments", column="payment_type", data_type="string", description="payment method (credit_card, boleto, voucher, debit_card)", pii="no", nullable="no"),
    Row(table="payments", column="payment_installments", data_type="integer", description="number of installments chosen by customer", pii="no", nullable="no"),
    Row(table="payments", column="payment_value", data_type="double", description="payment amount for this payment entry", pii="no", nullable="no"),
    # reviews
    Row(table="reviews", column="review_id", data_type="string", description="unique review identifier", pii="no", nullable="no"),
    Row(table="reviews", column="order_id", data_type="string", description="foreign key to orders table", pii="no", nullable="no"),
    Row(table="reviews", column="review_score", data_type="integer", description="rating from 1 to 5 given by customer", pii="no", nullable="no"),
    Row(table="reviews", column="review_comment_title", data_type="string", description="title of the review comment", pii="no", nullable="yes"),
    Row(table="reviews", column="review_comment_message", data_type="string", description="body text of the review", pii="no", nullable="yes"),
    Row(table="reviews", column="review_creation_date", data_type="timestamp", description="date when review was created", pii="no", nullable="no"),
    Row(table="reviews", column="review_answer_timestamp", data_type="timestamp", description="timestamp when review was answered", pii="no", nullable="no"),
    # products
    Row(table="products", column="product_id", data_type="string", description="unique product identifier", pii="no", nullable="no"),
    Row(table="products", column="product_category_name", data_type="string", description="product category name in portuguese", pii="no", nullable="yes"),
    Row(table="products", column="product_name_lenght", data_type="integer", description="character count of product name", pii="no", nullable="yes"),
    Row(table="products", column="product_description_lenght", data_type="integer", description="character count of product description", pii="no", nullable="yes"),
    Row(table="products", column="product_photos_qty", data_type="integer", description="number of product photos", pii="no", nullable="yes"),
    Row(table="products", column="product_weight_g", data_type="double", description="product weight in grams", pii="no", nullable="yes"),
    Row(table="products", column="product_length_cm", data_type="double", description="product length in centimeters", pii="no", nullable="yes"),
    Row(table="products", column="product_height_cm", data_type="double", description="product height in centimeters", pii="no", nullable="yes"),
    Row(table="products", column="product_width_cm", data_type="double", description="product width in centimeters", pii="no", nullable="yes"),
    # sellers
    Row(table="sellers", column="seller_id", data_type="string", description="unique seller identifier", pii="no", nullable="no"),
    Row(table="sellers", column="seller_zip_code_prefix", data_type="string", description="first 5 digits of seller zip code", pii="yes", nullable="no"),
    Row(table="sellers", column="seller_city", data_type="string", description="seller city name", pii="no", nullable="no"),
    Row(table="sellers", column="seller_state", data_type="string", description="seller state abbreviation", pii="no", nullable="no"),
    # geolocation
    Row(table="geolocation", column="geolocation_zip_code_prefix", data_type="string", description="zip code prefix for geo lookup", pii="no", nullable="no"),
    Row(table="geolocation", column="geolocation_lat", data_type="double", description="latitude coordinate", pii="no", nullable="no"),
    Row(table="geolocation", column="geolocation_lng", data_type="double", description="longitude coordinate", pii="no", nullable="no"),
    Row(table="geolocation", column="geolocation_city", data_type="string", description="city name for the zip prefix", pii="no", nullable="no"),
    Row(table="geolocation", column="geolocation_state", data_type="string", description="state abbreviation for the zip prefix", pii="no", nullable="no"),
    # category translation
    Row(table="translation", column="product_category_name", data_type="string", description="category name in portuguese", pii="no", nullable="no"),
    Row(table="translation", column="product_category_name_english", data_type="string", description="category name translated to english", pii="no", nullable="no"),
]

df_data_dict = spark.createDataFrame(data_dict_rows)
df_data_dict.createOrReplaceTempView("data_dictionary")
display(df_data_dict)

# COMMAND ----------

# A7.2 - Data dictionary (SQL)

display(spark.sql("""
    SELECT table, column, data_type, description, pii, nullable
    FROM data_dictionary
    ORDER BY table, column
"""))

# COMMAND ----------

# A7.3 - Data lineage documentation

lineage_rows = [
    # bronze layer - raw ingestion
    ("bronze", "orders_raw", "olist_orders_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "items_raw", "olist_order_items_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "customers_raw", "olist_customers_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "sellers_raw", "olist_sellers_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "products_raw", "olist_products_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "payments_raw", "olist_order_payments_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "reviews_raw", "olist_order_reviews_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "geo_raw", "olist_geolocation_dataset.csv", "A1.1", "raw csv load with schema inference"),
    ("bronze", "translation_raw", "product_category_name_translation.csv", "A1.1", "raw csv load with schema inference"),
    # silver layer - cleaned and validated
    ("silver", "orders_clean", "orders_raw", "A2.1-A2.4", "null handling, dedup, timestamp repair, date anomaly flags"),
    ("silver", "items_clean", "items_raw", "A2.1-A2.4", "dedup, invalid price/freight flags"),
    ("silver", "customers_clean", "customers_raw", "A2.3", "city/state standardized to consistent casing"),
    ("silver", "sellers_clean", "sellers_raw", "A2.3", "city/state standardized to consistent casing"),
    ("silver", "products_clean", "products_raw", "A2.1", "null category imputed with unknown"),
    ("silver", "payments_clean", "payments_raw", "A2.1-A2.2", "dedup, invalid payment flags"),
    ("silver", "reviews_clean", "reviews_raw", "A2.1", "null comments imputed with empty string"),
    ("silver", "geo_clean", "geo_raw", "A2.2-A2.3", "dedup, city/state standardized"),
    ("silver", "translation_clean", "translation_raw", "A2.2", "dedup only"),
    # gold layer - aggregates and features
    ("gold", "dim_geography", "geo_clean", "A3.1", "scd2 geography dimension keyed by zip prefix"),
    ("gold", "delivery_severity", "orders_clean + items_clean", "A4.1", "udf-based late-severity bucketing per delivered order"),
    ("gold", "seller_severity_dist", "delivery_severity", "A4.1", "late-severity distribution per seller"),
    ("gold", "category_delay_dist", "delivery_severity + products_clean + translation_clean", "A4.3", "p50/p90/p99 delivery delay per category"),
    ("gold", "seller_rolling_ontime", "delivery_severity", "A4.4", "7-day rolling on-time rate per seller"),
    ("gold", "payments_pivot", "payments_clean", "A4.5", "pivoted payment values by type per order"),
    ("gold", "customer_sessions", "orders_clean", "A5.3", "session count and avg orders per session per customer"),
    ("gold", "avg_score_state_month", "reviews_clean + orders_clean + customers_clean", "A5.4", "average review score by state and month"),
    ("gold", "freight_outliers", "items_clean", "A5.5", "freight outlier flags based on seller p95"),
    ("gold", "revenue_base", "items_clean + orders_clean + customers_clean + products_clean + translation_clean", "A6.1", "unified revenue table for dashboarding"),
]

df_lineage = spark.createDataFrame(lineage_rows, ["layer", "table_name", "source", "milestone", "transformation"])
df_lineage.createOrReplaceTempView("data_lineage")
display(df_lineage)

# COMMAND ----------

# A7.3 - Data lineage documentation (SQL)

display(spark.sql("""
    SELECT layer, table_name, source, milestone, transformation
    FROM data_lineage
    ORDER BY
        CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
        table_name
"""))

# COMMAND ----------

# A7.4 - Simulate RBAC grants

grants = [
    # data engineer grants
    "GRANT USAGE ON CATALOG main TO data_engineer",
    "GRANT USAGE ON SCHEMA main.default TO data_engineer",
    "GRANT SELECT, MODIFY ON SCHEMA main.default TO data_engineer",
    "GRANT CREATE TABLE ON SCHEMA main.default TO data_engineer",
    # data analyst grants
    "GRANT USAGE ON CATALOG main TO data_analyst",
    "GRANT USAGE ON SCHEMA main.default TO data_analyst",
    "GRANT SELECT ON SCHEMA main.default TO data_analyst",
    # data scientist grants
    "GRANT USAGE ON CATALOG main TO data_scientist",
    "GRANT USAGE ON SCHEMA main.default TO data_scientist",
    "GRANT SELECT ON SCHEMA main.default TO data_scientist",
    # business user grants - restricted to gold layer views only
    "GRANT USAGE ON CATALOG main TO business_user",
    "GRANT USAGE ON SCHEMA main.default TO business_user",
    "GRANT SELECT ON TABLE main.default.revenue_base TO business_user",
    "GRANT SELECT ON TABLE main.default.payments_pivot TO business_user",
    "GRANT SELECT ON TABLE main.default.category_delay_dist TO business_user",
    # admin grants
    "GRANT ALL PRIVILEGES ON CATALOG main TO admin",
    # column-level security: mask pii columns for non-admin roles
    "-- ALTER TABLE customers ALTER COLUMN customer_unique_id SET MASK mask_pii",
    "-- ALTER TABLE customers ALTER COLUMN customer_zip_code_prefix SET MASK mask_pii",
    "-- ALTER TABLE sellers ALTER COLUMN seller_zip_code_prefix SET MASK mask_pii",
    # row-level security example: analysts see only delivered orders
    "-- CREATE FUNCTION row_filter_delivered(status STRING) RETURN status = 'delivered'",
    "-- ALTER TABLE orders SET ROW FILTER row_filter_delivered ON (order_status) FOR data_analyst",
]

for g in grants:
    print(g)

print("\nnote: grant statements above are documented for reference.")
print("in a production environment, these would be executed by an admin user.")
print("column masking and row filters require unity catalog with appropriate privileges.")

# COMMAND ----------

# A7.4 - Simulate RBAC grants (SQL)

from pyspark.sql import Row

grant_rows = [
    Row(role="data_engineer", grant_statement="GRANT SELECT, MODIFY ON SCHEMA main.default", scope="all tables"),
    Row(role="data_analyst", grant_statement="GRANT SELECT ON SCHEMA main.default", scope="all tables read only"),
    Row(role="data_scientist", grant_statement="GRANT SELECT ON SCHEMA main.default", scope="all tables read only"),
    Row(role="business_user", grant_statement="GRANT SELECT ON TABLE revenue_base, payments_pivot, category_delay_dist", scope="gold layer only"),
    Row(role="admin", grant_statement="GRANT ALL PRIVILEGES ON CATALOG main", scope="full control"),
]

df_grants = spark.createDataFrame(grant_rows)
df_grants.createOrReplaceTempView("rbac_grants")

display(spark.sql("""
    SELECT role, grant_statement, scope
    FROM rbac_grants
    ORDER BY role
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A8: Advance SQL + PySpark Queries

# COMMAND ----------

# A8.1 - Monthly customer acquisition cohorts with 3-month retention by state

from pyspark.sql.functions import (
    col, min as spark_min, date_format, countDistinct,
    round as spark_round, months_between, floor
)

# find each customer's cohort month (first purchase month)
df_cohort = (df_orders_ts
    .join(df_customers_clean.select("customer_id", "customer_state"), on="customer_id", how="inner")
    .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM"))
)

df_first_purchase = (df_cohort
    .groupBy("customer_id")
    .agg(spark_min("order_month").alias("cohort_month"))
)

df_cohort = df_cohort.join(df_first_purchase, on="customer_id", how="inner")

# compute months since cohort month for each order
df_cohort = df_cohort.withColumn(
    "months_since_cohort",
    floor(months_between(
        col("order_month").cast("date"),
        col("cohort_month").cast("date")
    )).cast("int")
)

# count distinct customers per cohort, state, and months_since_cohort
df_cohort_counts = (df_cohort
    .groupBy("customer_state", "cohort_month", "months_since_cohort")
    .agg(countDistinct("customer_id").alias("customer_count"))
)

df_cohort_size = (df_cohort_counts
    .filter(col("months_since_cohort") == 0)
    .select(
        col("customer_state"),
        col("cohort_month"),
        col("customer_count").alias("cohort_size")
    )
)

# join and compute retention for months 1, 2, 3
df_retention = (df_cohort_counts
    .filter(col("months_since_cohort").isin(1, 2, 3))
    .join(df_cohort_size, on=["customer_state", "cohort_month"], how="inner")
    .withColumn("retention_pct", spark_round((col("customer_count") / col("cohort_size")) * 100, 2))
    .orderBy("customer_state", "cohort_month", "months_since_cohort")
)

df_retention.createOrReplaceTempView("customer_cohort_retention")
display(df_retention)

# COMMAND ----------

# A8.1 - Monthly customer acquisition cohorts with 3-month retention by state (SQL)

display(spark.sql("""
    WITH orders_with_state AS (
        SELECT o.order_id, o.customer_id, c.customer_state,
            date_format(o.order_purchase_timestamp, 'yyyy-MM') AS order_month
        FROM orders_ts o
        JOIN customers c ON o.customer_id = c.customer_id
    ),
    first_purchase AS (
        SELECT customer_id, MIN(order_month) AS cohort_month
        FROM orders_with_state
        GROUP BY customer_id
    ),
    with_cohort AS (
        SELECT ows.*, fp.cohort_month,
            CAST(FLOOR(months_between(CAST(ows.order_month AS DATE), CAST(fp.cohort_month AS DATE))) AS INT) AS months_since_cohort
        FROM orders_with_state ows
        JOIN first_purchase fp ON ows.customer_id = fp.customer_id
    ),
    cohort_counts AS (
        SELECT customer_state, cohort_month, months_since_cohort,
            COUNT(DISTINCT customer_id) AS customer_count
        FROM with_cohort
        GROUP BY customer_state, cohort_month, months_since_cohort
    ),
    cohort_size AS (
        SELECT customer_state, cohort_month, customer_count AS cohort_size
        FROM cohort_counts
        WHERE months_since_cohort = 0
    )
    SELECT
        cc.customer_state,
        cc.cohort_month,
        cc.months_since_cohort,
        cc.customer_count,
        cs.cohort_size,
        ROUND((cc.customer_count / cs.cohort_size) * 100, 2) AS retention_pct
    FROM cohort_counts cc
    JOIN cohort_size cs ON cc.customer_state = cs.customer_state AND cc.cohort_month = cs.cohort_month
    WHERE cc.months_since_cohort IN (1, 2, 3)
    ORDER BY cc.customer_state, cc.cohort_month, cc.months_since_cohort
"""))

# COMMAND ----------

# A8.2 - Seller rolling 30-day revenue and MoM growth ranked by state

from pyspark.sql.functions import (
    sum as spark_sum, lag, col, round as spark_round, date_format, when
)
from pyspark.sql.window import Window
from pyspark.sql.functions import rank as spark_rank

# build seller monthly revenue joined with seller state
df_seller_monthly = (df_items_clean
    .join(df_orders_ts.select("order_id", "order_purchase_timestamp"), on="order_id", how="inner")
    .join(df_sellers_clean.select("seller_id", "seller_state"), on="seller_id", how="inner")
    .withColumn("order_month", date_format("order_purchase_timestamp", "yyyy-MM"))
    .groupBy("seller_id", "seller_state", "order_month")
    .agg(spark_sum("price").alias("monthly_revenue"))
)

# rolling 30-day revenue using a 1-month window (approximation: month-level granularity)
w_seller = Window.partitionBy("seller_id").orderBy("order_month")

df_seller_growth = (df_seller_monthly
    .withColumn("prev_month_revenue", lag("monthly_revenue", 1).over(w_seller))
    .withColumn("mom_growth_pct",
        when(col("prev_month_revenue") > 0,
            spark_round(((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100, 2)
        ).otherwise(None)
    )
)

# rank sellers by mom growth per state and month
w_rank = Window.partitionBy("seller_state", "order_month").orderBy(col("mom_growth_pct").desc())

df_seller_growth = df_seller_growth.withColumn("growth_rank", spark_rank().over(w_rank))

df_seller_growth.createOrReplaceTempView("seller_mom_growth")
display(df_seller_growth.orderBy("seller_state", "order_month", "growth_rank"))

# COMMAND ----------

# A8.2 - Seller rolling 30-day revenue and MoM growth ranked by state (SQL)

display(spark.sql("""
    WITH seller_monthly AS (
        SELECT
            i.seller_id,
            s.seller_state,
            date_format(o.order_purchase_timestamp, 'yyyy-MM') AS order_month,
            SUM(i.price) AS monthly_revenue
        FROM items i
        JOIN orders_ts o ON i.order_id = o.order_id
        JOIN sellers s ON i.seller_id = s.seller_id
        GROUP BY i.seller_id, s.seller_state, date_format(o.order_purchase_timestamp, 'yyyy-MM')
    ),
    with_growth AS (
        SELECT *,
            LAG(monthly_revenue, 1) OVER (PARTITION BY seller_id ORDER BY order_month) AS prev_month_revenue,
            CASE
                WHEN LAG(monthly_revenue, 1) OVER (PARTITION BY seller_id ORDER BY order_month) > 0
                THEN ROUND(((monthly_revenue - LAG(monthly_revenue, 1) OVER (PARTITION BY seller_id ORDER BY order_month))
                    / LAG(monthly_revenue, 1) OVER (PARTITION BY seller_id ORDER BY order_month)) * 100, 2)
                ELSE NULL
            END AS mom_growth_pct
        FROM seller_monthly
    )
    SELECT *,
        RANK() OVER (PARTITION BY seller_state, order_month ORDER BY mom_growth_pct DESC) AS growth_rank
    FROM with_growth
    ORDER BY seller_state, order_month, growth_rank
"""))

# COMMAND ----------

# A8.3 - RFM scoring per customer over last 180 days

from pyspark.sql.functions import (
    max as spark_max, count, sum as spark_sum, datediff, lit, col,
    round as spark_round, ntile, current_date
)
from pyspark.sql.window import Window

ref_date = df_orders_ts.agg(spark_max("order_purchase_timestamp")).collect()[0][0]

# filter to last 180 days from reference date
df_recent = (df_orders_ts
    .filter(datediff(lit(ref_date), col("order_purchase_timestamp")) <= 180)
)

# join with items for monetary calculation
df_rfm_base = (df_recent
    .join(df_items_clean.select("order_id", "price", "freight_value"), on="order_id", how="inner")
    .withColumn("revenue", col("price") + col("freight_value"))
)

df_rfm = (df_rfm_base
    .groupBy("customer_id")
    .agg(
        datediff(lit(ref_date), spark_max("order_purchase_timestamp")).alias("recency"),
        countDistinct("order_id").alias("frequency"),
        spark_round(spark_sum("revenue"), 2).alias("monetary")
    )
)

# assign deciles (1-10) for each metric
w_recency = Window.orderBy(col("recency").asc())
w_frequency = Window.orderBy(col("frequency").desc())
w_monetary = Window.orderBy(col("monetary").desc())

df_rfm = (df_rfm
    .withColumn("r_decile", ntile(10).over(w_recency))
    .withColumn("f_decile", ntile(10).over(w_frequency))
    .withColumn("m_decile", ntile(10).over(w_monetary))
)

# composite score: average of the three deciles (lower = better customer)
df_rfm = df_rfm.withColumn(
    "rfm_score", spark_round((col("r_decile") + col("f_decile") + col("m_decile")) / 3.0, 2)
)

df_rfm.createOrReplaceTempView("customer_rfm")
display(df_rfm.orderBy("rfm_score"))

# COMMAND ----------

# A8.3 - RFM scoring per customer over last 180 days (SQL)

display(spark.sql("""
    WITH ref AS (
        SELECT MAX(order_purchase_timestamp) AS ref_date FROM orders_ts
    ),
    recent_orders AS (
        SELECT o.*
        FROM orders_ts o, ref r
        WHERE datediff(r.ref_date, o.order_purchase_timestamp) <= 180
    ),
    rfm_base AS (
        SELECT
            ro.customer_id,
            datediff(r.ref_date, MAX(ro.order_purchase_timestamp)) AS recency,
            COUNT(DISTINCT ro.order_id) AS frequency,
            ROUND(SUM(i.price + i.freight_value), 2) AS monetary
        FROM recent_orders ro
        JOIN items i ON ro.order_id = i.order_id
        CROSS JOIN ref r
        GROUP BY ro.customer_id, r.ref_date
    )
    SELECT *,
        NTILE(10) OVER (ORDER BY recency ASC) AS r_decile,
        NTILE(10) OVER (ORDER BY frequency DESC) AS f_decile,
        NTILE(10) OVER (ORDER BY monetary DESC) AS m_decile,
        ROUND((
            NTILE(10) OVER (ORDER BY recency ASC) +
            NTILE(10) OVER (ORDER BY frequency DESC) +
            NTILE(10) OVER (ORDER BY monetary DESC)
        ) / 3.0, 2) AS rfm_score
    FROM rfm_base
    ORDER BY rfm_score
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Milestone A9: Orchestration and Automation 

# COMMAND ----------

# A9.1 - Create Databricks Job for pipeline orchestration
# milestones A1-A10 are already modularized within the notebook with clear cell separations

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask

w = WorkspaceClient()

notebook_path = "/Workspace/Users/shashank.w.yadav@gmail.com/DataEngineeringAssignment"

job = w.jobs.create(
    name="Olist_DEA_Pipeline",
    tasks=[
        Task(task_key="run_full_pipeline",
             description="execute all milestones A1-A10 in sequence",
             notebook_task=NotebookTask(notebook_path=notebook_path))
    ]
)

print(f"job created successfully. job id: {job.job_id}")

# COMMAND ----------

# A9.1 - Pipeline DAG design documentation
# in production, each milestone would be a separate notebook with this dependency structure

from pyspark.sql import Row

dag_rows = [
    Row(task="A1_data_ingestion", depends_on="none", description="load csvs with schema inference and explicit schemas"),
    Row(task="A2_data_cleansing", depends_on="A1_data_ingestion", description="null handling, dedup, type/range checks"),
    Row(task="A3_enrichment_cdc", depends_on="A2_data_cleansing", description="scd2, timestamp repair, trailing on-time, funnel"),
    Row(task="A4_business_aggregates", depends_on="A3_enrichment_cdc", description="late-severity, pivots, percentiles, rolling on-time"),
    Row(task="A5_performance_optimization", depends_on="A4_business_aggregates", description="broadcast, z-order, sessions, reviews, freight"),
    Row(task="A6_visualization", depends_on="A5_performance_optimization", description="kpi, bar, line, pie, box charts"),
    Row(task="A7_governance_security", depends_on="A6_visualization", description="rbac, data dictionary, lineage, grants"),
    Row(task="A8_advanced_queries", depends_on="A5_performance_optimization", description="cohort retention, mom growth, rfm"),
]

df_dag = spark.createDataFrame(dag_rows)
df_dag.createOrReplaceTempView("pipeline_dag")

# production note:
# to implement as a multi-task job, each milestone would be extracted into its own notebook
# and write intermediate results to delta tables instead of using in-memory dataframes
# this enables independent retries, parallel execution (A6/A7 and A8 in parallel), and better monitoring

display(df_dag)

# COMMAND ----------

# A9.2 - Real failure and recovery scenario
# intentionally read from a wrong path, catch error, retry with correct path

import time

BASE_PATH = "/Volumes/main/default/dataengineeringassignment/"
WRONG_PATH = "/Volumes/main/default/dataengineeringassignment/nonexistent_file.csv"
CORRECT_PATH = BASE_PATH + "olist_orders_dataset.csv"

max_retries = 3
retry_delay = 2

def load_with_retry(path, fallback_path, retries, delay):
    attempt = 0
    last_error = None
    while attempt < retries:
        try:
            attempt += 1
            print(f"attempt {attempt}: reading from {path}")
            df = spark.read.option("header", True).option("inferSchema", True).csv(path)
            row_count = df.count()
            print(f"success: loaded {row_count} rows from {path}")
            return df
        except Exception as e:
            last_error = str(e)
            print(f"attempt {attempt} failed: {last_error[:150]}")
            if attempt < retries:
                print(f"retrying in {delay} seconds with fallback path...")
                time.sleep(delay)
                path = fallback_path
                delay = delay * 2
            else:
                print(f"all {retries} retries exhausted.")
                raise

# run the failure and recovery
print("starting failure and recovery demonstration\n")

df_recovered = load_with_retry(WRONG_PATH, CORRECT_PATH, max_retries, retry_delay)

print(f"\nrecovery successful. dataframe schema:")
df_recovered.printSchema()

# COMMAND ----------

# A9.2 - Real failure and recovery scenario (SQL)
# log the failure and recovery details

from pyspark.sql import Row
from datetime import datetime

recovery_log = [
    Row(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        task="load_orders",
        attempt=1,
        path=WRONG_PATH,
        status="failed",
        error="file not found at nonexistent path",
        action="retry with correct path"
    ),
    Row(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        task="load_orders",
        attempt=2,
        path=CORRECT_PATH,
        status="success",
        error="none",
        action="loaded successfully after recovery"
    ),
]

df_recovery_log = spark.createDataFrame(recovery_log)
df_recovery_log.createOrReplaceTempView("recovery_log")

display(spark.sql("""
    SELECT timestamp, task, attempt, path, status, error, action
    FROM recovery_log
    ORDER BY attempt
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### A9.3 - Failure and recovery documentation
# MAGIC
# MAGIC - failure scenario:
# MAGIC   - task: load orders dataset from volumes
# MAGIC   - cause: incorrect file path (/nonexistent_file.csv) passed to spark.read.csv
# MAGIC   - error type: AnalysisException - path does not exist
# MAGIC
# MAGIC - recovery strategy:
# MAGIC   1. try/except wraps every data load operation
# MAGIC   2. on failure, log the error with task name, attempt number, and timestamp
# MAGIC   3. retry with fallback path (correct path) after a delay
# MAGIC   4. exponential backoff: delay doubles on each retry (2s, 4s, 8s)
# MAGIC   5. max retries set to 3 before giving up
# MAGIC
# MAGIC - production recommendations:
# MAGIC   - in databricks jobs, configure "max retries" at the task level (settings > retries)
# MAGIC   - set up email/slack alerts on task failure via job notification settings
# MAGIC   - use job clusters with auto-termination to avoid cost on stuck tasks
# MAGIC   - store recovery logs in a delta table for audit trail
# MAGIC   - for data quality failures, use expectations (databricks dlt) to quarantine bad records instead of failing the entire pipeline
# MAGIC
# MAGIC - what happened in our simulation:
# MAGIC   - attempt 1: read from /Volumes/.../nonexistent_file.csv -> failed (file not found)
# MAGIC   - attempt 2: read from /Volumes/.../olist_orders_dataset.csv -> success (loaded 99441 rows)
# MAGIC   - total recovery time: ~2 seconds