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

# Load dataset
products_df = spark.read.csv("/data/olist/olist_products_dataset.csv", 
                             header=True, 
                             schema=StructType([
                                    StructField("product_id", StringType(), True),
                                    StructField("product_category_name", StringType(), True),
                                    StructField("product_name_lenght", IntegerType(), True),
                                    StructField("product_description_lenght", IntegerType(), True),
                                    StructField("product_photos_qty", IntegerType(), True),
                                    StructField("product_weight_g", IntegerType(), True),
                                    StructField("product_length_cm", IntegerType(), True),
                                    StructField("product_height_cm", IntegerType(), True),
                                    StructField("product_width_cm", IntegerType(), True)
                                ])
                                )

reviews_df = spark.read.csv("/data/olist/olist_order_reviews_dataset.csv", 
                            header=True, 
                            schema=StructType([
                                   StructField("review_id", StringType(), True),
                                   StructField("order_id", StringType(), True),
                                   StructField("review_score", IntegerType(), True),
                                   StructField("review_comment_title", StringType(), True),
                                   StructField("review_comment_message", StringType(), True),
                                   StructField("review_creation_date", TimestampType(), True),
                                   StructField("review_answer_timestamp", TimestampType(), True)
                                ])
                                )

items_df = spark.read.csv("/data/olist/olist_order_items_dataset.csv", 
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

# Gabungkan items dan review berdasarkan order_id, lalu join dengan products berdasarkan product_id
product_reviews_df = items_df.join(reviews_df, on="order_id", how="inner").join(products_df, on="product_id", how="inner")

# Agregasi rata-rata skor review
review_score_by_category = product_reviews_df.groupBy("order_id", "seller_id", "product_id", "review_id", "product_category_name").agg(
    avg("review_score").alias("avg_review_score")
)

#load data ke postgresql
(
    review_score_by_category
    .write
    .mode("overwrite")  # Use overwrite mode to replace the table
    .jdbc(
        jdbc_url,
        'public.olist_Analisis_Skor_Review_Rata_rata_Berdasarkan_Kategori_Produk',  # Table name
        properties=jdbc_properties
    )
)

#baca data
result_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_Analisis_Skor_Review_Rata_rata_Berdasarkan_Kategori_Produk',
    properties=jdbc_properties
)

result_df.show(10)