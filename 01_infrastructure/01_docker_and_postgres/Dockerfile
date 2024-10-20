#image docker dasar
FROM python:3.9.1

#menjalankan instalasi library pandas di image ini
RUN pip install pandas

#mengatur direktori kerja di dalam container
WORKDIR /app

#mendifinisikan apa yang akan dilakukan pertama kali ketika container dijalankan
COPY pipeline.py pipeline.py

#mendefinisikan apa yang akan dilakukan pertama kali ketika container dijalankan
#dalam contoh ini, kita akan menjalankan skrip pipeline.py
ENTRYPOINT [ "python", "pipeline.py" ]