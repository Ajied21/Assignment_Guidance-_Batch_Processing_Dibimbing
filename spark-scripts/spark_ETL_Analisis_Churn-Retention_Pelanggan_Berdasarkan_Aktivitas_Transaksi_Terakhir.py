import os
from pathlib import Path
import pyspark
from pyspark.sql.functions import col, when, max, avg, sum, count, date_sub, lit
from pyspark.sql.types import *
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Spark configuration
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

# Load datasets
olist_customers_dataset = spark.read.csv("/data/olist/olist_customers_dataset.csv", 
                                        header=True, 
                                        schema=StructType([
                                        StructField("customer_id", StringType(), True),
                                        StructField("customer_unique_id", StringType(), True),
                                        StructField("customer_zip_code_prefix", StringType(), True),
                                        StructField("customer_city", StringType(), True),
                                        StructField("customer_state", StringType(), True)
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

olist_order_reviews_dataset = spark.read.csv("/data/olist/olist_order_reviews_dataset.csv", 
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

# Filter hanya pesanan yang sudah selesai untuk menghitung pembelian berulang
completed_orders_df = olist_orders_dataset.filter(olist_orders_dataset.order_status == "delivered")

# Gabungkan orders dengan customers berdasarkan customer_id
orders_customers_df = completed_orders_df.join(olist_customers_dataset, on="customer_id", how="inner")

# Tentukan tanggal akhir data untuk menghitung churn (misalnya, 30 hari dari transaksi terakhir)
end_date = datetime.strptime("2018-08-29", "%Y-%m-%d")  # Misalkan data terakhir hingga 29 Agustus 2018
lookback_period = 30  # Periode 30 hari sebagai cutoff untuk churn

# Hitung tanggal terakhir pembelian setiap pelanggan
last_purchase_df = orders_customers_df.groupBy("customer_unique_id").agg(
    max("order_purchase_timestamp").alias("last_purchase_date")
)

# Tandai pelanggan sebagai churn atau retained berdasarkan aktivitas dalam 30 hari terakhir
churn_analysis_df = last_purchase_df.withColumn(
    "churned",
    when(col("last_purchase_date") < date_sub(lit(end_date), lookback_period), 1).otherwise(0)
)

# Gabungkan churn status ke tabel pelanggan untuk mengetahui churn-retention rate
customers_churn_df = olist_customers_dataset.join(churn_analysis_df, on="customer_unique_id", how="left").fillna(0)

# Hitung total pelanggan churn dan retained
churn_summary = customers_churn_df.groupBy("churned").agg(
    count("customer_unique_id").alias("customer_count")
)

# Konversikan kolom churned menjadi kategori churned atau retained untuk kejelasan
churn_summary = churn_summary.withColumn(
    "status",
    when(col("churned") == 1, "churned").otherwise("retained")
).drop("churned")

(
    churn_summary
    .write
    .mode("overwrite")  # Use overwrite mode to replace the table
    .jdbc(
        jdbc_url,
        'public.olist_Analisis_Churn_Retention_Pelanggan_Berdasarkan_Aktivitas_Transaksi_Terakhir',  # Table name
        properties=jdbc_properties
    )
)

result_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_Analisis_Churn_Retention_Pelanggan_Berdasarkan_Aktivitas_Transaksi_Terakhir',
    properties=jdbc_properties
)

result_df.show(10)