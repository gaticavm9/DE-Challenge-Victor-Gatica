from typing import List, Tuple
from datetime import datetime

import pandas as pd
from collections import defaultdict, Counter

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Encuentra las 10 fechas con más tweets y el usuario con más publicaciones en cada una de esas fechas.
    Esta función busca optimizar el uso de memoria en la ejecución.

    Args:
        file_path (str): Ruta al archivo JSON que contiene los datos de los tweets.

    Returns:
        List[Tuple[datetime.date, str]]: Una lista de tuplas donde cada tupla contiene una fecha y el usuario con más tweets en esa fecha.
    """
    chunk_size = 1000
    date_counts = Counter()
    user_counts_by_date = defaultdict(Counter)
    
    for chunk in pd.read_json(file_path, lines=True, chunksize=chunk_size, dtype={
        "date":"datetime64[ns]",
        "user":"object"
    }):
        chunk['date'] = pd.to_datetime(chunk['date']).dt.date
        
        # Actualizar el contador de fechas
        date_counts.update(chunk['date'])
        # Actualizar el contador de usuarios por fecha
        for date, user in zip(chunk['date'], chunk['user']):
            user_counts_by_date[date][user['username']] += 1
    
    # Obtener las 10 fechas con más tweets
    top_dates = date_counts.most_common(10)
    
    result = [(date, user_counts_by_date[date].most_common(1)[0][0]) for date, _ in top_dates]
    
    return result

