import pytest
from pyspark.sql import SparkSession
from sales_job import (
    join_orders_and_items,
    # join_with_payments,
    # join_with_customers,
    # join_with_geolocation,
    # join_with_sellers
)

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("pytest-pyspark-sales").getOrCreate()
    yield spark
    spark.stop()


def test_join_orders_and_items(spark):
    orders = [
        ("o1", "delivered", "2023-01-01", "2023-01-02", "2023-01-03", "cust1"),
    ]
    orders_schema = ["order_id","order_status","order_approved_at","order_delivered_carrier_date","order_date","customer_id"]
    df_orders = spark.createDataFrame(orders, orders_schema)

    order_items = [
        ("o1", 1, "p1", "2023-01-05", 10.0),
    ]
    items_schema = ["order_id","order_item_id","product_id","shipping_limit_date","freight_value"]
    df_items = spark.createDataFrame(order_items, items_schema)

    result = join_orders_and_items(df_orders, df_items)
    assert "order_status" not in result.columns
    assert result.filter(result.order_id == "o1").count() == 1


# def test_join_with_payments(spark):
#     orders_items = [
#         ("o1", "2023-01-03", "cust1", "p1")
#     ]
#     orders_items_schema = ["order_id", "order_date", "customer_id", "product_id"]
#     df_orders_items = spark.createDataFrame(orders_items, orders_items_schema)

#     payments = [
#         ("o1", 1, "credit_card", 100.0)
#     ]
#     payments_schema = ["order_id", "payment_sequential", "payment_type", "payment_value"]
#     df_payments = spark.createDataFrame(payments, payments_schema)

#     result = join_with_payments(df_orders_items, df_payments)
#     assert "payment_sequential" not in result.columns
#     assert result.filter(result.payment_type == "credit_card").count() == 1


# def test_join_with_customers(spark):
#     orders_payments = [
#         ("o1", "2023-01-03", "cust1", "p1", "credit_card", 100.0)
#     ]
#     schema_orders_payments = ["order_id", "order_date", "customer_id", "product_id", "payment_type", "payment_value"]
#     df_orders_payments = spark.createDataFrame(orders_payments, schema_orders_payments)

#     customers = [
#         ("cust1", "SP", "city1")
#     ]
#     schema_customers = ["customer_id", "customer_state", "customer_city"]
#     df_customers = spark.createDataFrame(customers, schema_customers)

#     result = join_with_customers(df_orders_payments, df_customers)
#     assert result.filter(result.customer_state == "SP").count() == 1


# def test_join_with_geolocation(spark):
#     customers_data = [
#         ("o1", "2023-01-03", "cust1", "p1", "credit_card", 100.0, "SP", "city1")
#     ]
#     schema_customers_data = ["order_id", "order_date", "customer_id", "product_id", "payment_type", "payment_value", "customer_state", "customer_city"]
#     df_customers_data = spark.createDataFrame(customers_data, schema_customers_data)

#     geolocation = [
#         ("SP", "city1", -23.55, -46.63)
#     ]
#     schema_geo = ["geolocation_state", "geolocation_city", "latitude", "longitude"]
#     df_geo = spark.createDataFrame(geolocation, schema_geo)

#     result = join_with_geolocation(df_customers_data, df_geo)
#     assert result.filter(result.latitude == -23.55).count() == 1


# def test_join_with_sellers(spark):
#     geo_data = [
#         ("o1", "2023-01-03", "cust1", "p1", "credit_card", 100.0, "SP", "city1", -23.55, -46.63)
#     ]
#     schema_geo_data = ["order_id", "order_date", "customer_id", "product_id", "payment_type", "payment_value", "customer_state", "customer_city", "latitude", "longitude"]
#     df_geo_data = spark.createDataFrame(geo_data, schema_geo_data)

#     sellers = [
#         ("s1", "SP", "city1")
#     ]
#     schema_sellers = ["seller_id", "seller_state", "seller_city"]
#     df_sellers = spark.createDataFrame(sellers, schema_sellers)

#     result = join_with_sellers(df_geo_data, df_sellers)
#     assert "seller_id" in result.columns
