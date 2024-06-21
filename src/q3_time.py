from typing import List, Tuple

import polars as pl
import re

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los top 10 usuarios más mencionados con su respectivo conteo.
    Esta función busca optimizar el tiempo de ejecución utilizando Polars.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas donde cada tupla contiene un usuario y el conteo de veces que se mencionó.
        [("LATAM321", 387), ("LATAM_CHI", 129), ...]
    """

    df = pl.scan_ndjson(file_path)
    df = df.explode('mentionedUsers')
    df = df.select(
        pl.col("mentionedUsers").apply(lambda x: x['username'], return_dtype=pl.String).alias("username")
    ).filter(
        pl.col("username") != []
    )
    # Agrupar por menciones y contar las ocurrencias, luego obtener el top 10
    df = df.groupby("username").agg(pl.count().alias("count"))
    df = df.sort("count", descending=True).limit(10)
    # Convertir el LazyFrame a DataFrame
    df = df.collect()
    result = list(zip(df["username"].to_list(), df["count"].to_list()))
    return result



def extract_mentions(text: str) -> List[str]:
    """
    Extrae todas las menciones (@usuario) de un texto.

    Args:
        text (str): Texto del que se extraerán las menciones.

    Returns:
        List[str]: Lista de menciones extraídas.
    """
    return re.findall(r'@(\w+)', text)

def q3_time2(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los top 10 usuarios más mencionados con su respectivo conteo.
    Esta función busca optimizar el tiempo de ejecución utilizando Polars.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas donde cada tupla contiene un usuario y el conteo de veces que se mencionó.
        [("LATAM321", 387), ("LATAM_CHI", 129), ...]
    """

    df = pl.scan_ndjson(file_path)
    df = df.select(
        pl.col("content").apply(extract_mentions, return_dtype=pl.List(str)).alias("mentions")
    ).filter(
        pl.col("mentions") != []
    )
    df = df.explode("mentions")
    # Agrupar por menciones y contar las ocurrencias, luego obtener el top 10
    df = df.groupby("mentions").agg(pl.count().alias("count"))
    df = df.sort("count", descending=True).limit(10)
    # Convertir el LazyFrame a DataFrame
    df = df.collect()
    result = list(zip(df["mentions"].to_list(), df["count"].to_list()))
    return result