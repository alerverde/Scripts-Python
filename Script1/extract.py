# extract.py
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Cargar variables de entorno y establecer conexión con la base de datos
load_dotenv()
ORIGIN_DB_URL = os.getenv("ORIGIN_DB_URL")
engine = create_engine(ORIGIN_DB_URL)

# Tablas a extraer de la base de datos origen
TABLE_NAMES = ["DimDate", "DimCustomerSegment", "DimProduct", "FactSales"]

# Diccionario donde se almacenan los DataFrames extraídos
dataframes = {}
print("Extrayendo tablas desde la base origen...\n")

# Extraer cada tabla y guardarla en el diccionario
for table in TABLE_NAMES:
    try:
        df = pd.read_sql(f'SELECT * FROM "{table}"', engine)
        dataframes[table] = df
        print(f"{table} extraída: {len(df)} filas")
    except Exception as e:
        print(f"Error extrayendo {table}: {e}")

print("\nExtracción finalizada.")