from typing import List, Tuple

import emoji
import pandas as pd
from collections import Counter

def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Encuentra los top 10 emojis más usados con su respectivo conteo.
    Esta función busca optimizar el uso de memoria en la ejecución.

    Args:
        file_path (str): Ruta al archivo JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[str, int]]: Una lista de tuplas donde cada tupla contiene un emoji y su respectivo conteo.
    """
    chunk_size = 1000
    emoji_counts = Counter()
    for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size, dtype={"content":"text"}):
        # Extraer los emojis de cada tweet y actualizar el contador de emojis
        for tweet in chunk['content']:
            emojis = [emo['emoji'] for emo in emoji.emoji_list(tweet)]
            emoji_counts.update(emojis)
    # Obtener los 10 emojis más comunes
    top_emojis = emoji_counts.most_common(10)
    return top_emojis