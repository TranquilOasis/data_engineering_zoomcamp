import sys

import pandas as pd

# Read in the data

print(sys.argv)

day = sys.argv[1]

print(f"Reading in the data...{day}")




# services:
#   postgres:
#     image: postgres:13
#     environment:
#       POSTGRES_USER: airflow
#       POSTGRES_PASSWORD: airflow
#       POSTGRES_DB: airflow
#     volumes:
#       - postgres-db-volume:/var/lib/postgresql/data
#     healthcheck:
#       test: ["CMD", "pg_isready", "-U", "airflow"]
#       interval: 5s
#       retries: 5
#     restart: always

