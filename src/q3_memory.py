from typing import List, Tuple


import pandas as pd
from collections import defaultdict, Counter

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los top 10 usuarios más mencionados con su respectivo conteo.
    Esta función busca optimizar el tiempo de ejecución utilizando Polars.

    Args:
        file_path (str): Ruta del JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas donde cada tupla contiene un usuario y el conteo de veces que se mencionó.
        [("LATAM321", 387), ("LATAM_CHI", 129), ...]
    """
    chunk_size = 1000
    user_mention_counts = Counter()
    
    for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size, dtype={"mentionedUsers": "object"}):
        chunk = chunk.explode("mentionedUsers").dropna(subset=["mentionedUsers"])
        chunk['username'] = chunk['mentionedUsers'].apply(lambda x: x['username'])
        user_mention_counts.update(chunk['username'])

    result = user_mention_counts.most_common(10)
    
    return result