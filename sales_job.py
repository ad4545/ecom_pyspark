from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date
import logging

# 1. Initialize SparkSession: This is the entry point for all Spark functionality.
spark = SparkSession.builder \
    .appName("PySparkSalesJob") \
    .getOrCreate()

df_orders = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_orders_dataset.csv",header=True,inferSchema=True)
df_products = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_products_dataset.csv",header=True,inferSchema=True)
df_customers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_customers_dataset.csv",header=True,inferSchema=True)
df_sellers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_sellers_dataset.csv",header=True,inferSchema=True)
df_order_items = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_items_dataset.csv",header=True,inferSchema=True)
df_order_paymemts = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_payments_dataset.csv",header=True,inferSchema=True)

drop_orders_column = ["order_status","order_approved_at","order_delivered_carrier_date"]

df_dropped_orders = df_orders.drop(*drop_orders_column)

order_product_df =  df_dropped_orders.join(df_order_items,on="order_id",how="left")

drop_joined_column = ["order_item_id","shipping_limit_date","freight_value"]

df_sales_initial = order_product_df.drop(*drop_joined_column)

df_sales_with_category = df_sales_initial.join(df_products,on="product_id",how="left")

drop_sales_column = ["product_id","product_name_lenght","product_description_lenght","product_weight_g","product_length_cm","product_height_cm","product_width_cm"]

df_dropped_sales = df_sales_with_category.drop(*drop_sales_column)

df_sales_renamed = df_dropped_sales.withColumnsRenamed({"order_id":"sales_id","product_category_name":"product_category","product_photos_qty":"unit_sold"})

df_sales_date_improved = df_sales_renamed.withColumn("date",to_date("order_purchase_timestamp","yyyy-MM-dd HH:mm:ss"))

drop_sales_date_improved = ["order_purchase_timestamp","order_delivered_customer_date","order_estimated_delivery_date"]

df_dropped_improved = df_sales_date_improved.drop(*drop_sales_date_improved)

df_sales_filtered = df_dropped_improved.drop(*["price","unit_sold"])

df_joined_payments = df_sales_filtered.join(df_order_paymemts,df_sales_filtered["sales_id"]==df_order_paymemts["order_id"],"left")

df_sales_payment_filtered = df_joined_payments.drop(*["payment_sequential","payment_type","payment_installments"])

df_sales_with_revenue = df_sales_payment_filtered.withColumnRenamed("payment_value","revenue")

df_sales_customers_joined = df_sales_with_revenue.join(df_customers,on="customer_id",how="left")

resulted_joined = df_sales_customers_joined.drop(*["customer_unique_id","customer_zip_code_prefix"])

resulted_joined_renamed = resulted_joined.withColumnsRenamed({"customer_city":"city","customer_state":"state"})

df_final_sales_table = resulted_joined_renamed.drop(*["order_id"])

sfOptions = {
  "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
  "sfUser": "AD07",
  "sfPassword": "Adarsh06572307011",
  "sfDatabase": "ECOM_DB",
  "sfSchema": "ECOM_SCHEMA",
  "sfWarehouse": "ECOM_WH",
}

df_final_sales_table.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable", "ECOM_SCHEMA.SALES").mode("overwrite").save()

print("Spark Job for Sales table Done.....")

spark.stop()
