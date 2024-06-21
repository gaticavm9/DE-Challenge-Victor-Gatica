from typing import List, Tuple
from datetime import datetime

import polars as pl
import emoji
from collections import Counter
import re


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los top 10 emojis más usados con su respectivo conteo.
    Esta función busca optimizar el tiempo de ejecución utilizando Polars.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas donde cada tupla contiene un emoji y el conteo de veces que se usó.
        [("✈️", 6856), ("❤️", 5876), ...]
    """

    df = pl.scan_ndjson(file_path)
    # Preprocesamiento: eliminar URLs, menciones de usuario y hashtags
    df = df.select(
        pl.col("content").apply(lambda text: re.sub(r"(?:\@|https?\://)\S+", "", text), return_dtype=pl.String).alias("content")
    )
    df = df.select(
        pl.col("content").apply(extract_emojis,return_dtype=pl.List(str)).alias("emojis")
    ).filter(
        # Filas que no tienen emojis
        pl.col("emojis") != []
    )
    df = df.explode("emojis")
    df = df.groupby("emojis").agg(pl.count().alias("count"))
    df = df.sort("count", descending=True).limit(10)
    df = df.collect()
    result = list(zip(df["emojis"], df["count"]))
    return result


def extract_emojis(text: str) -> List[str]:
    """
    Extrae todos los emojis de un texto y los devuelve en una lista.

    Args:
        text (str): Texto del que se extraerán los emojis.

    Returns:
        List[str]: Lista de emojis extraídos.
    """
    return [emj['emoji'] for emj in emoji.emoji_list(text)]

