import os
from pathlib import Path
import pyspark
from pyspark.sql.functions import col,when, avg, sum, count
from pyspark.sql.types import *
from dotenv import load_dotenv

dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster(spark_host)
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

#load datasets
olist_order_items_dataset = spark.read.csv("/data/olist/olist_order_items_dataset.csv", 
                                           header=True, 
                                           schema=StructType([
                                                  StructField("order_id", StringType(), True),
                                                  StructField("order_item_id", StringType(), True),
                                                  StructField("product_id", StringType(), True),
                                                  StructField("seller_id", StringType(), True),
                                                  StructField("shipping_limit_date", TimestampType(), True),
                                                  StructField("price", DoubleType(), True),
                                                  StructField("freight_value", DoubleType(), True)
                                                 ])
                                                 )

olist_orders_dataset = spark.read.csv("/data/olist/olist_orders_dataset.csv", 
                                      header=True, 
                                      schema=StructType([
                                             StructField("order_id", StringType(), True),
                                             StructField("customer_id", StringType(), True),
                                             StructField("order_status", StringType(), True),
                                             StructField("order_purchase_timestamp", TimestampType(), True),
                                             StructField("order_approved_at", TimestampType(), True),
                                             StructField("order_delivered_carrier_date", TimestampType(), True),
                                             StructField("order_delivered_customer_date", TimestampType(), True),
                                             StructField("order_estimated_delivery_date", TimestampType(), True)
                                            ])
                                            )

# Join datasets on order_id
sales_df = olist_order_items_dataset.join(olist_orders_dataset, on="order_id", how="inner")

# Aggregation: Total price and freight value
sales_agg_df = sales_df.groupBy("order_id", "product_id", "seller_id", "customer_id", "order_status").agg(
    sum("price").alias("total_sales"),
    sum("freight_value").alias("total_freight"),
    avg("price").alias("avg_sales"),
    avg("freight_value").alias("avg_freight")
)

(
    sales_agg_df
    .write
    .mode("overwrite")  # Use overwrite mode to replace the table
    .jdbc(
        jdbc_url,
        'public.olist_Analisis_Penjualan_dan_Nilai_Total_Pengiriman',  # Table name
        properties=jdbc_properties
    )
)

result_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_Analisis_Penjualan_dan_Nilai_Total_Pengiriman',
    properties=jdbc_properties
)

result_df.show(10)
