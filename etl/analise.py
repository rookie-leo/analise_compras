from pyspark.shell import spark

from etl.parquet_converter.parquet_converter import json_to_parquet


class AnaliseCompras:

    def execute(self, path: str):
        # self.__verifica_extensao(path)
        self.__extract()
        pass

    def __extract(self):
        spark.read.parquet("/home/leonardo/porjetos_python/analise_compras/result/2023-06-05/vendas").show()
        pass

    def __verifica_extensao(self, path: str) -> None:
        file_name = path.split("/")[-1].split(".")[-2]
        extension = path.split(".")[-1]

        try:
            if extension == "json":
                json_to_parquet(path, file_name)

        except NameError as ex:
            print("Extensão não cadastrada")
