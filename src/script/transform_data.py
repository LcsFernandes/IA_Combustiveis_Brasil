import polars as pl
import os
from dotenv import load_dotenv

load_dotenv()
URL_DATA_BRONZE = os.getenv("URL_DATA_BRONZE")
URL_DATA_SILVER = os.getenv("URL_DATA_SILVER")

if not URL_DATA_BRONZE:
    raise Exception("URL_DATA_BRONZE nao encontrado")

if not URL_DATA_SILVER:
    raise Exception("URL_DATA_SILVER nao encontrado")


def get_data():
    try:
        path = URL_DATA_BRONZE

        df_temp = pl.DataFrame()
        df = pl.DataFrame()

        for file in os.listdir(path):           
            if file.endswith(".parquet"):
                df_temp = pl.read_parquet(f"{path}/{file}")
                df_temp = df_temp.with_columns("numero_rua").cast(str)

                df = pl.concat([df, df_temp])

        return df
    except Exception as exception:
        raise Exception(f"Erro ao extrair os dados historicos de combustiveis do arquivo {file}. mensagem: {exception}")
    

def transforma_data():

    df = get_data()

    df = df.select([
        pl.col("regiao_sigla").cast(pl.Utf8),
        pl.col("estado_sigla").cast(pl.Utf8),
        pl.col("municipio").cast(pl.Utf8),
        pl.col("revenda").cast(pl.Utf8),
        pl.col("cnpj_da_revenda").str.replace_all(r"\D", "").cast(pl.Utf8),
        pl.col("bairro").cast(pl.Utf8),
        pl.col("produto").cast(pl.Utf8),
        pl.col("data_da_coleta").str.strptime(pl.Date, "%d/%m/%Y", strict=False),
        pl.col("valor_de_venda").str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("valor_de_compra").str.replace(",", ".").cast(pl.Float64, strict=False),
        pl.col("unidade_de_medida").cast(pl.Utf8),
        pl.col("bandeira").cast(pl.Utf8)
    ])

    df = df.filter(
        (pl.col("regiao_sigla").is_not_null()) &
        (pl.col("valor_de_venda").is_not_null())
    ).with_columns(
        pl.when(pl.col("regiao_sigla") == "S").then(pl.lit("Sul"))
      .when(pl.col("regiao_sigla") == "SE").then(pl.lit("Sudeste"))
      .when(pl.col("regiao_sigla") == "CO").then(pl.lit("Centro-Oeste"))
      .when(pl.col("regiao_sigla") == "NE").then(pl.lit("Nordeste"))
      .when(pl.col("regiao_sigla") == "N").then(pl.lit("Norte"))
      .alias("regiao")
    )

    print(df.head())
    return df

def load_data():

    path = URL_DATA_SILVER  
    df = transforma_data()
    df.write_parquet(path)