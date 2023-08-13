import os
import pyarrow.parquet as pq
import pandas as pd

def parquet_to_pandasDF(path: str) -> pd.DataFrame:
    trips = pq.read_table(path)

    trips_df = trips.to_pandas()


def parquet_to_csvfile(url: str, path: str) -> None:
    trips = pq.read_table(url)

    trips_df = trips.to_pandas()

    trips_df.to_csv(f'{path}', index=False)

