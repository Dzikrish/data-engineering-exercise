import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time #mengimpor modul time untuk tracking waktu

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    dbName = params.dbName
    tableName = params.tableName
    url = params.url

    if url.endswith('.csv.gz'):
        csvName = 'output.csv.gz'
    else:
        csvName = 'output.csv'

    # download dataset
    os.system(f"wget {url} -O {csvName}")

    print(user, password, host, port, dbName)
    #format url: 'postgresql://[user]:[password]@[host]:[port]/[dbname]'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbName}')

    #membagi data dalam chunk
    if url.endswith('.csv.gz'):
        df_iter = pd.read_csv(csvName, iterator=True, chunksize=100000, compression ='gzip')
    else:
        df_iter = pd.read_csv(csvName, iterator=True, chunksize=100000)


    # Mendapatkan chunk pertama dan menyimpannya ke dalam variabel df
    df=next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #def.head untuk mengambil kepala kolom, con ini menggunakan engine apa, dan if_exists kalo ada yang sama langsung di replace
    df.head(n=0).to_sql(name=tableName,con=engine, if_exists='replace')
    df.to_sql(name=tableName, con=engine, if_exists='append')

    #loop yang akan berjalan terus menerus sampai terjadi exception(seperti StopIteration)
    while True:
        try:
            t_start = time() # Menandai waktu mulai proses
            df = next(df_iter) # Mengambil chunk berikutnya dari terator df_iter
            
            # Mengubah kolom tpep_pickup_datetime dan tpep_dropoff_datetime
            # dari format string ke datetime menggunakan pandas.to_datetime
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            # Menyimpan DataFrame ke dalam tabel SQL 'yellow_taxi_data'.
            # - 'con=engine' menggunakan engine SQLAlchemy yang telah dibuat sebelumnya untuk koneksi database.
            # - 'if_exist='append'' berarti bahwa data akan ditambahkan ke tabel jika tabel itu sudah ada.
            df.to_sql(name=tableName, con=engine, if_exists='append')
            
            t_end = time() # Menandai waktu akhir proses
            
            # Mencetak waktu yang diperlukan untuk memproses dan memasukkan chunk data tersebut
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('Finished ingesting data into the postgres database')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres database')
    parser.add_argument('--user',required=True, help='Username for postgres database')
    parser.add_argument('--password',required=True, help='Password for postgres database')
    parser.add_argument('--host',required=True, help='Hostname for postgres database')
    parser.add_argument('--port',required=True, help='Port for postgres database')
    parser.add_argument('--dbName',required=True, help='Database name for postgres database')
    parser.add_argument('--tableName',required=True, help='Table name for postgres database')
    parser.add_argument('--url',required=True, help='Url for postgres database')
    args = parser.parse_args()
    main(args)