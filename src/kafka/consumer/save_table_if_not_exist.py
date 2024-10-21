from delta.tables import *
from pyspark.sql import SparkSession


def save_table_if_not_exists(spark: SparkSession, table_name: str, dataframe: DataFrame) -> None:
    # default_path = "file:/home/dinho/workspace/ada/minio-test/src/spark/spark-warehouse/"
    # delta_table_path = f"file:/home/dinho/workspace/ada/minio-test/src/spark/spark-warehouse/{table_name}"
    source_bucket = "raw-ada" 
    delta_path = f"s3a://{source_bucket}/{table_name}"

    if DeltaTable.isDeltaTable(spark, delta_path):

        #Merge de arquivos não duplicados
        # Carregar a tabela Delta com notícias previamente armazenadas
        delta_table = DeltaTable.forPath(spark, delta_path)

        # Usar o MERGE para inserir apenas as notícias que ainda não existem
        delta_table.alias("old").merge(
            dataframe.alias("new"),
            "old.url = new.url"
        ).whenNotMatchedInsertAll().execute()

    else:
        #Salvar em delta
        # dataframe.write.format("delta").saveAsTable(table_name)

        dataframe.write.format("delta").mode("overwrite").save(delta_path)