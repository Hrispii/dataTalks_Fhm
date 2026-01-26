import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user') # @click.option('--pg-user', 'pg_user' default='root', help='PostgreSQL user') # --pg-user в консоли; pg_user название этого в функции, по умолчанию оно отрезает -- и - заменяет на _
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi_dataset', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_dataset', help='Target table name')
@click.option('--url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', help='URL of the CSV file')


def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, url):


    prefix = url
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    df_iter = pd.read_csv(
        prefix, # path in the internet
        
        iterator=True, # with next comant works toogether. It converts dataframe into "queue", Now result won't be a finished DataFrame, but an iterator object.
        chunksize=100000
    )


    try:
        first_chunk = next(df_iter) # first 100.000 lines
    except StopIteration:
        print("File is empty")
        return 

    first_chunk.head(0).to_sql(  # create table, if exist -> replace
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(  # insert first 100.000 lines
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in tqdm(df_iter): # tqdm - progress bar, due to using nest() df_iter understand that first 100.000 lines was already taken and starts with second 100.000
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))

    pass


if __name__ == '__main__':
    run()