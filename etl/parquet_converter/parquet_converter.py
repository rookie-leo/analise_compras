from datetime import date

from pyspark.shell import spark
from pyspark.sql import DataFrame


def json_to_parquet(path: str, file_name: str) -> DataFrame:
    print("Iniciando conversão")
    data = date.today()
    parquet = spark\
        .read.json(path)\
        .write.parquet(f"../result/{data}/{file_name}")

    print("Conversão realizada com sucesso")

    return parquet


