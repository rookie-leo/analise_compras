from etl.analise import AnaliseCompras
from etl.parquet_converter.parquet_converter import json_to_parquet


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # json_to_parquet("/home/leonardo/porjetos_python/analise_compras/json/vendas.json", "vendas")
    analise = AnaliseCompras()

    analise.execute("/home/leonardo/porjetos_python/analise_compras/json/vendas.json")
