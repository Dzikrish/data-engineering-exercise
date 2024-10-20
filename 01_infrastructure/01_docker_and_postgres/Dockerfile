#image docker dasar
FROM python:3.9.1

#menjalankan instalasi library pandas di image ini
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

#mengatur direktori kerja di dalam container
WORKDIR /app

#mendifinisikan apa yang akan dilakukan pertama kali ketika container dijalankan
COPY ingest-data.py ingest-data.py

#mendefinisikan apa yang akan dilakukan pertama kali ketika container dijalankan
#dalam contoh ini, kita akan menjalankan skrip pipeline.py
ENTRYPOINT [ "python", "ingest-data.py" ]