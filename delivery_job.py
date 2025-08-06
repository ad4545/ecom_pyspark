from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,datediff

spark = SparkSession.builder.appName("PySparkDeliveryJob").getOrCreate()

df_orders = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_orders_dataset.csv",header=True,inferSchema=True)
df_products = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_products_dataset.csv",header=True,inferSchema=True)
df_customers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_customers_dataset.csv",header=True,inferSchema=True)
df_sellers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_sellers_dataset.csv",header=True,inferSchema=True)
df_order_items = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_items_dataset.csv",header=True,inferSchema=True)
df_reviews = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_reviews_dataset.csv",header=True,inferSchema=True)

df_joined_orders = df_orders.join(df_order_items,on="order_id",how="inner")
df_selected_orders = df_joined_orders.select(col("order_id"),col("customer_id"),col("order_purchase_timestamp").alias("order_purchase_date"),col("order_delivered_customer_date").alias("delivered_date"),col("order_estimated_delivery_date").alias("expected_delivery_date"),col("product_id"),col("seller_id"))

df_deliver_with_dates = df_selected_orders.withColumn("delivered_date",to_date("delivered_date","yyyy-MM-dd")).withColumn("expected_delivery_date",to_date("expected_delivery_date","yyyy-MM-dd")).withColumn("order_purchase_date",to_date("order_purchase_date","yyyy-MM-dd"))

df_delivery_transformed = df_deliver_with_dates.withColumn("delivery_time_days",datediff(df_deliver_with_dates["delivered_date"],df_deliver_with_dates["order_purchase_date"])).withColumn("delivery_delay_days",datediff(df_deliver_with_dates["delivered_date"],df_deliver_with_dates["expected_delivery_date"]))

df_delivery_final = df_delivery_transformed.drop("order_purchase_date","delivered_date","expected_delivery_date")

sfOptions = {
  "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
  "sfUser": "AD07",
  "sfPassword": "Adarsh06572307011",
  "sfDatabase": "ECOM_DB",
  "sfSchema": "ECOM_SCHEMA",
  "sfWarehouse": "ECOM_WH",
}

df_delivery_final.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option("dbtable", "ECOM_SCHEMA.DELIVERY").mode("overwrite").save()

print("Spark Job for Delivery table Done.....")

spark.stop()