from pipeline import run
import pyarrow.parquet as pq
import duckdb

if __name__ == "__main__":
	run()
 
con = duckdb.connect()

df = con.execute("SELECT* FROM 'data/staging/weather_warsaw.parquet' WHERE date >= now ()").df()

print(df)


#1. zapis do bazy na gcp i cron
#1a. zapytania sql
#2. Krok po kroku w konsoli GCP
#3. zad2. buduj warsty raw -> stag -> final