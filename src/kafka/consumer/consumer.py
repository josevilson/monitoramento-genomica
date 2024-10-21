# %%
from confluent_kafka import Consumer, KafkaException
from save_table_if_not_exist import save_table_if_not_exists
import json
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from delta.tables import *
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




# Settings Kafka
kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Endereço do servidor Kafka
    'group.id': 'news-consumer-group',      # Grupo de consumidores
    'auto.offset.reset': 'earliest'         # Começa a consumir do início se não houver commit de offset
}



# Consumer
consumer = Consumer(kafka_consumer_config)

# Topic configs
topic_name = 'ada-apresentacao'
consumer.subscribe([topic_name])

print(f"Consumidor Kafka escutando o tópico '{topic_name}'...")

# Função para processar as mensagens consumidas
def process_message(message):
    try:
        article = json.loads(message.value().decode('utf-8'))
 
        df = spark.createDataFrame([article])
        df.show()
        source_bucket = "raw-ada" 

        table_name = "ada-foi"
        save_table_if_not_exists(spark,table_name, df)

        print("Arquivo delta enviado com sucesso para o MinIO.")
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar mensagem JSON: {e}")

# Loop de consumo contínuo
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Processar a mensagem
            process_message(msg)

except KeyboardInterrupt:
    print("Consumo interrompido.")

finally:
    consumer.close()
