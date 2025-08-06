from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col,row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySparkBusinessJob").getOrCreate()

df_orders = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_orders_dataset.csv",header=True,inferSchema=True)
df_products = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_products_dataset.csv",header=True,inferSchema=True)
df_customers = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_customers_dataset.csv",header=True,inferSchema=True)
df_order_items = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_items_dataset.csv",header=True,inferSchema=True)
df_reviews = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_reviews_dataset.csv",header=True,inferSchema=True)
df_payments = spark.read.csv("s3a://spark-app-storage-raw/raw_ecom_data/olist_order_payments_dataset.csv",header=True,inferSchema=True)

df_statewise_total = df_customers.groupBy("customer_state").agg(count("customer_id").alias("total_customers")).orderBy("customer_state")

df_cust_orders = df_customers.join(df_orders,on="customer_id",how="inner")

df_cust_orders_payments = df_cust_orders.join(df_payments,on="order_id",how="inner")

df_count_payments = df_cust_orders_payments.groupBy("customer_state","payment_type").agg(count("order_id").alias("uses"))

state_window = Window.partitionBy("customer_state").orderBy(col("uses").desc())

df_statewise_payments = df_count_payments.withColumn("rank", row_number().over(state_window)).filter(col("rank") == 1).select("customer_state", "payment_type").orderBy("customer_state")

df_joined_category = df_order_items.join(df_products,on="product_id",how="inner")

df_joined_category_scores = df_joined_category.join(df_reviews,on="order_id",how="left")

df_joined_category_final = df_joined_category_scores.groupBy("product_category_name").agg(count("review_score").alias("score"))

df_joined_category_desc = df_joined_category_final.orderBy(col("score").desc())

sfOptionsForStateWiseCustomer = {
  "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
  "sfUser": "AD07",
  "sfPassword": "Adarsh06572307011",
  "sfDatabase": "ECOM_DB",
  "sfSchema": "ECOM_SCHEMA",
  "sfWarehouse": "ECOM_WH",
}

sfOptionsForStateWisePayments = {
  "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
  "sfUser": "AD07",
  "sfPassword": "Adarsh06572307011",
  "sfDatabase": "ECOM_DB",
  "sfSchema": "ECOM_SCHEMA",
  "sfWarehouse": "ECOM_WH",
}

sfOptionsForCategoryWiseReviewScore = {
  "sfURL": "NAHZTMI-HZ73012.snowflakecomputing.com",
  "sfUser": "AD07",
  "sfPassword": "Adarsh06572307011",
  "sfDatabase": "ECOM_DB",
  "sfSchema": "ECOM_SCHEMA",
  "sfWarehouse": "ECOM_WH",
}

df_statewise_total.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForStateWiseCustomer).option("dbtable", "ECOM_SCHEMA.STATEWISECUSTOMER").mode("overwrite").save()

df_statewise_payments.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForStateWisePayments).option("dbtable", "ECOM_SCHEMA.STATEWISEPAYMENT").mode("overwrite").save()

df_joined_category_desc.write.format("net.snowflake.spark.snowflake").options(**sfOptionsForCategoryWiseReviewScore).option("dbtable", "ECOM_SCHEMA.CATEGORYWISEREVIEWSCORE").mode("overwrite").save()

print("Spark Job for Business tables Done.....")

spark.stop()