import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user') # @click.option('--pg-user', 'pg_user' default='root', help='PostgreSQL user') # --pg-user в консоли; pg_user название этого в функции, по умолчанию оно отрезает -- и - заменяет на _
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--url', default='https://d37ci6vzurychx.cloudfront.net/trip-data/', help='URL of the parquet file')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, url):
    prefix = url
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


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
        "congestion_surcharge": "float64"
    }

    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]

    df_iter = pd.read_parquet(prefix + 'green_tripdata_2025-11.parquet')


    df_iter.head(0).to_sql(  # create table, if exist -> replace
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    df_iter.to_sql(  
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted", len(df_iter))

    pass


if __name__ == '__main__':
    run()