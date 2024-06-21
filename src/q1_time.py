from typing import List, Tuple
from datetime import datetime

import pandas as pd
import polars as pl

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones en cada una de esas fechas.
    Esta función busca optimizar el tiempo de ejecución utilizando Polars.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas donde cada tupla contiene una fecha y el usuario con más tweets en esa fecha.
        [(datetime.date(2021, 2, 12), 'RanbirS00614606'), (datetime.date(2021, 2, 13), 'MaanDee08215437'), ...]
    """

    df = pl.scan_ndjson(file_path)
    df = df.select(
            pl.col("date").cast(pl.Datetime).cast(pl.Date).alias("date"),
            # sacamos el username dentro del objeto user
            pl.col("user").struct.field("username").alias("username"),
    )
    df = df.collect()
    top_fechas = df.groupby('date').agg(pl.count('*').alias('count')).sort('count', descending=True).head(10)['date']

    result = []
    for date in top_fechas:
        tweets_diarios = df.filter(pl.col('date') == date)
        top_usuario = tweets_diarios.groupby('username').agg(pl.count('*').alias('count')).sort('count', descending=True).head(1)['username'][0]
        result.append((date, top_usuario))
    return result


def q1_time_pandas(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones en cada una de esas fechas.
    Esta función busca optimizar el tiempo de ejecución.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas donde cada tupla contiene una fecha y el usuario con más tweets en esa fecha.
        [(datetime.date(2021, 2, 12), 'RanbirS00614606'), (datetime.date(2021, 2, 13), 'MaanDee08215437'), ...]
    """

    df = pd.read_json(file_path, lines=True, dtype={
        "date":"datetime64[ns]",
        "user":"object"
    })
    df['date'] = df['date'].dt.date
    
    # Agrupamos por fecha y contamos la cantidad de tweets, nos quedamos con el top 10
    top_dates = df['date'].value_counts().nlargest(10).index
    
    result = []
    for date in top_dates:
        daily_tweets = df[df['date'] == date]
        # Buscamos el usuario con más tweets el día que estamos evaluando
        top_user = daily_tweets['user'].apply(lambda x: x['username']).value_counts().idxmax()
        result.append((date, top_user))
    
    return result