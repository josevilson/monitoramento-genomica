# %%
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from delta.tables import *
from delta import *

builder = SparkSession.builder \
    .appName("Exemplo Delta Lake") \
    .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.2.1.jar") \
    .config("spark.jars", "/opt/spark/jars/delta-storage-4.0.0.jar") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Criar um DataFrame de exemplo
data = [("Ana", 29), ("Bruno", 34), ("Carlos", 24)]
columns = ["Nome", "Idade"]
df = spark.createDataFrame(data, columns)


##

delta_table_path = "file:/home/dinho/workspace/ada/minio-test/src/spark/spark-warehouse/second"
if DeltaTable.isDeltaTable(spark, delta_table_path):
    #Merge de arquivos não duplicados

    # Carregar a tabela Delta com notícias previamente armazenadas
    delta_table = DeltaTable.forPath(spark, delta_table_path)

 # Usar o MERGE para inserir apenas as notícias que ainda não existem
    delta_table.alias("old").merge(
        df.alias("new"),
        "old.Nome = new.Nome AND old.Idade = new.Idade"
    ).whenNotMatchedInsertAll().execute()

else:
    #Salvar em delta
    df.write.format("delta").saveAsTable("second")


# ##


# try:
#     df.write.format("delta").saveAsTable("first")
# except Exception as e:
#     print(e)
# # %%
