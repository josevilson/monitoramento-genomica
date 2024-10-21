from delta.tables import *
from pyspark.sql import SparkSession


def save_table_if_not_exists(spark: SparkSession, table_name: str, dataframe: DataFrame) -> None:
    default_path = "file:/home/dinho/workspace/ada/minio-test/src/spark/spark-warehouse/"

    delta_table_path = f"file:/home/dinho/workspace/ada/minio-test/src/spark/spark-warehouse/{table_name}"

    if DeltaTable.isDeltaTable(spark, delta_table_path):

        #Merge de arquivos não duplicados
        # Carregar a tabela Delta com notícias previamente armazenadas
        delta_table = DeltaTable.forPath(spark, delta_table_path)

        # Usar o MERGE para inserir apenas as notícias que ainda não existem
        delta_table.alias("old").merge(
            dataframe.alias("new"),
            "old.Nome = new.Nome AND old.Idade = new.Idade"
        ).whenNotMatchedInsertAll().execute()

    else:
        #Salvar em delta
        dataframe.write.format("delta").saveAsTable(table_name)