# Imports
from delta.tables import *
from save_table_if_not_exist import save_table_if_not_exists
from confluent_kafka import Consumer, KafkaException
import json
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from delta import *


# Spark Session - Inicialização.
builder = SparkSession.builder \
    .appName("Enviar DataFrame Simples para MinIO") \
    .config("spark.jars", "/opt/spark/jars/hadoop-aws-3.3.1.jar,/opt/spark/jars/sdk-core-2.20.0.jar,/opt/spark/jars/s3-2.20.0.jar") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.2.1.jar") \
    .config("spark.jars", "/opt/spark/jars/delta-storage-4.0.0.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "9vzcvkYRiMnUPheIIwCo") \
    .config("spark.hadoop.fs.s3a.secret.key", "eYTGt3XKCUxg5y3hSFZyJomSzK4t4LPe4oXPmneb") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")\

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read Bucket with Delta.

source_bucket = "raw-ada" 

table_name = "ada-foi"

delta_table_path = f"s3a://{source_bucket}/{table_name}"


df = spark.read.format("delta").load(delta_table_path)

# Show DF
df.show()