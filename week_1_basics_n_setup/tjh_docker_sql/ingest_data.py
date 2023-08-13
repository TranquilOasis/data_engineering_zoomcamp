#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine
    
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    # Get csv
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100000,
        low_memory=False,
    )

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df["tpep_pickup_datetime"])
    df.tpep_dropoff_datetime = pd.to_datetime(df["tpep_dropoff_datetime"])


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    df.to_sql(name=table_name, con=engine, if_exists="append", index=False)


    while True:
        t_start = time()

        try:
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df["tpep_pickup_datetime"])
            df.tpep_dropoff_datetime = pd.to_datetime(df["tpep_dropoff_datetime"])

            df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

            t_end = time()

            print(f"Chunk loaded in {t_end - t_start:.3f} seconds.")

        except StopIteration:
            print("Done!")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ingest csv data to postgresql')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url to scv file')

    args = parser.parse_args()

    main(args)