#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
    "trip_type": "Int64",
    "ehail_fee": "float64",
}


@click.command()
@click.option("--pg-user", default="root", help="PostgreSQL user")
@click.option("--pg-pass", default="root", help="PostgreSQL password")
@click.option("--pg-host", default="localhost", help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default="ny_taxi", help="PostgreSQL database name")
@click.option("--year", default=2025, type=int, help="Year of the data")
@click.option("--month", default=11, type=int, help="Month of the data")
@click.option("--target-table", default="green_taxi_trips", help="Target table name")
@click.option(
    "--chunksize", default=100000, type=int, help="Chunk size for reading parquet"
)
def run(
    pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize
):
    """Ingest NYC green taxi data into PostgreSQL database."""
    file_path = f"./green_tripdata_{year}-{month:02d}.parquet"

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # Read parquet file
    df = pd.read_parquet(file_path, dtype_backend="pyarrow")

    # Process in chunks
    total_rows = len(df)
    first = True

    for start in tqdm(range(0, total_rows, chunksize)):
        df_chunk = df.iloc[start : start + chunksize]

        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
