import os
import sys


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("LAB10_SparkSQL") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "-Djava.security.manager=allow") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession uruchomiona.\n")

print("=" * 60)
print("ZADANIE 1: Wczytanie danych (symulacja Parquet)")
print("=" * 60)

sales_data = [
    ("2024-01-05", "Warszawa",  "Elektronika", "Laptop",    3499.99, 2),
    ("2024-01-12", "Krakow",    "AGD",          "Lodowka",  1899.00, 1),
    ("2024-01-18", "Gdansk",    "Elektronika", "Smartfon",   899.50, 5),
    ("2024-02-03", "Warszawa",  "Odziez",       "Kurtka",    349.00, 3),
    ("2024-02-14", "Wroclaw",   "Elektronika", "Tablet",    1249.99, 4),
    ("2024-02-20", "Krakow",    "AGD",          "Pralka",   1599.00, 1),
    ("2024-03-07", "Gdansk",    "Odziez",       "Buty",      199.99, 6),
    ("2024-03-15", "Warszawa",  "Elektronika", "Laptop",    3299.00, 1),
    ("2024-03-22", "Wroclaw",   "AGD",          "Zmywarka", 1150.00, 2),
    ("2024-04-01", "Krakow",    "Elektronika", "Smartfon",   799.00, 3),
    ("2024-04-10", "Warszawa",  "Odziez",       "Spodnie",   129.99, 8),
    ("2024-04-25", "Gdansk",    "AGD",          "Lodowka",  1750.00, 1),
]

sales_schema = StructType([
    StructField("data",      StringType(),  True),
    StructField("miasto",    StringType(),  True),
    StructField("kategoria", StringType(),  True),
    StructField("produkt",   StringType(),  True),
    StructField("cena",      DoubleType(),  True),
    StructField("ilosc",     IntegerType(), True),
])

df = spark.createDataFrame(sales_data, schema=sales_schema)

print(">>> show() – pierwsze wiersze:")
df.show(5)

print(">>> printSchema():")
df.printSchema()

print("=" * 60)
print("ZADANIE 2: Dane klientów (symulacja CSV) + widok tymczasowy")
print("=" * 60)

klienci_data = [
    ("K001", "Anna",      "Kowalska",     "Warszawa", 34, "Premium"),
    ("K002", "Piotr",     "Nowak",        "Krakow",   28, "Standard"),
    ("K003", "Maria",     "Wisniewska",   "Gdansk",   45, "Premium"),
    ("K004", "Tomasz",    "Wojcik",       "Wroclaw",  52, "Standard"),
    ("K005", "Ewa",       "Kaminska",     "Warszawa", 31, "VIP"),
    ("K006", "Marek",     "Lewandowski",  "Krakow",   40, "Standard"),
    ("K007", "Katarzyna", "Dabrowska",    "Gdansk",   27, "Premium"),
    ("K008", "Jakub",     "Zielinski",    "Wroclaw",  38, "VIP"),
    ("K009", "Agnieszka", "Szymanska",    "Warszawa", 55, "Standard"),
    ("K010", "Michal",    "Wozniak",      "Krakow",   22, "Standard"),
]

klienci_schema = StructType([
    StructField("klient_id", StringType(),  True),
    StructField("imie",      StringType(),  True),
    StructField("nazwisko",  StringType(),  True),
    StructField("miasto",    StringType(),  True),
    StructField("wiek",      IntegerType(), True),
    StructField("segment",   StringType(),  True),
])

df_klienci = spark.createDataFrame(klienci_data, schema=klienci_schema)

print(">>> show() – dane klientów:")
df_klienci.show()

print(">>> printSchema():")
df_klienci.printSchema()

df_klienci.createOrReplaceTempView("klienci")
df.createOrReplaceTempView("sprzedaz")
print("Widoki tymczasowe 'klienci' i 'sprzedaz' zarejestrowane.\n")

print(">>> SELECT * FROM klienci LIMIT 10:")
spark.sql("SELECT * FROM klienci LIMIT 10").show()

print("=" * 60)
print("ZADANIE 3: Spark SQL – zapytania zaawansowane")
print("=" * 60)

print("--- 3.1 Agregacje (SUM, AVG, COUNT, MAX, MIN) ---")
spark.sql("""
    SELECT
        COUNT(*)                            AS liczba_transakcji,
        ROUND(SUM(cena * ilosc), 2)         AS laczny_przychod,
        ROUND(AVG(cena), 2)                 AS srednia_cena,
        MAX(cena)                           AS max_cena,
        MIN(cena)                           AS min_cena
    FROM sprzedaz
""").show()

print("--- 3.2 Grupowanie po kategorii i mieście (GROUP BY) ---")
spark.sql("""
    SELECT
        kategoria,
        miasto,
        COUNT(*)                    AS liczba_transakcji,
        ROUND(SUM(cena * ilosc), 2) AS przychod,
        ROUND(AVG(cena), 2)         AS srednia_cena
    FROM sprzedaz
    GROUP BY kategoria, miasto
    ORDER BY przychod DESC
""").show()

print("--- 3.3 Filtrowanie: cena > 1000 PLN ---")
spark.sql("""
    SELECT *,
           ROUND(cena * ilosc, 2) AS wartosc_zamowienia
    FROM sprzedaz
    WHERE cena > 1000
    ORDER BY wartosc_zamowienia DESC
""").show()

print("--- 3.4 JOIN: zamówienia + klienci ---")

zamowienia_data = [
    ("ZAM001", "K001", "Laptop",    3499.99),
    ("ZAM002", "K002", "Lodowka",   1899.00),
    ("ZAM003", "K003", "Smartfon",   899.50),
    ("ZAM004", "K005", "Tablet",    1249.99),
    ("ZAM005", "K006", "Pralka",    1599.00),
    ("ZAM006", "K007", "Buty",       199.99),
    ("ZAM007", "K004", "Zmywarka",  1150.00),
]

zamowienia_schema = StructType([
    StructField("zamowienie_id", StringType(), True),
    StructField("klient_id",     StringType(), True),
    StructField("produkt",       StringType(), True),
    StructField("kwota",         DoubleType(), True),
])

spark.createDataFrame(zamowienia_data, schema=zamowienia_schema) \
     .createOrReplaceTempView("zamowienia")

spark.sql("""
    SELECT
        z.zamowienie_id,
        k.imie,
        k.nazwisko,
        k.segment,
        k.miasto,
        z.produkt,
        z.kwota
    FROM zamowienia z
    JOIN klienci k ON z.klient_id = k.klient_id
    ORDER BY z.kwota DESC
""").show()

print("=" * 60)
print("ZAPIS WYNIKÓW DO PLIKU CSV")
print("=" * 60)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

wynik_group = spark.sql("""
    SELECT
        kategoria,
        miasto,
        COUNT(*)                    AS liczba_transakcji,
        ROUND(SUM(cena * ilosc), 2) AS przychod,
        ROUND(AVG(cena), 2)         AS srednia_cena
    FROM sprzedaz
    GROUP BY kategoria, miasto
    ORDER BY przychod DESC
""")

wynik_join = spark.sql("""
    SELECT
        z.zamowienie_id,
        k.imie,
        k.nazwisko,
        k.segment,
        k.miasto,
        z.produkt,
        z.kwota
    FROM zamowienia z
    JOIN klienci k ON z.klient_id = k.klient_id
    ORDER BY z.kwota DESC
""")

path_group = os.path.join(BASE_DIR, "wynik_grupowanie.csv")
path_join  = os.path.join(BASE_DIR, "wynik_join.csv")

wynik_group.toPandas().to_csv(path_group, index=False, encoding="utf-8")
wynik_join.toPandas().to_csv(path_join,   index=False, encoding="utf-8")

print(f"Zapisano: {path_group}")
print(f"Zapisano: {path_join}")

print("\nWszystkie zadania wykonane pomyślnie!")
spark.stop()
print("SparkSession zatrzymana.")