from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey
)
import pandas as pd
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert  # para ON CONFLICT
from sqlalchemy.orm import Session

from schema import metadata
from schema import get_engine
from schema import DimDate, DimCustomerSegment, DimProduct, FactSales
import os
from dotenv import load_dotenv

# Cargar variables de entorno y establecer conexión con la base de datos
load_dotenv()
ORIGIN_DB_URL="postgresql://dbt_user:dbt_pass@localhost:5432/dbt_db"
engine = get_engine(ORIGIN_DB_URL)

# Crear tablas si no existen
try:
    metadata.create_all(engine)
    print("Tablas creadas correctamente.")
except Exception as e:
    print(f"Error creando las tablas: {e}")

# Insertamos registros nuevos o actualiza si hubo modificaciones
def upsert_from_csv(engine, table, csv_path, pk_column):
    """
    Inserta datos desde un archivo CSV en una tabla de la base de datos.
    Si el registro ya existe (según clave primaria), se actualiza.
    Args:
        engine (sqlalchemy.Engine): Conexión a la base de datos
        table (sqlalchemy.Table): Tabla objetivo
        csv_path (str): Ruta al archivo CSV
        pk_column (str): Nombre de la columna de clave primaria
    """
    try:
        # Leer y limpiar el CSV
        df = pd.read_csv(csv_path)
        # Convertir todos los nombres de columnas a minúsculas
        # df.columns = df.columns.str.lower()
        df = df.where(pd.notnull(df), None) # Reemplaza NaNs por None para PostgreSQL
        
        # Filtra columnas válidas según la tabla        
        table_columns = set(c.name for c in table.columns)
        df = df[[col for col in df.columns if col in table_columns]]

        with Session(engine) as session:
            for _, row in df.iterrows():
                stmt = pg_insert(table).values(**row.to_dict())
                
                # Prepara actualización si hay conflicto de PK
                update_cols = {
                    c.name: stmt.excluded[c.name]
                    for c in table.columns
                    if c.name != pk_column
                }

                stmt = stmt.on_conflict_do_update(
                    index_elements=[pk_column],
                    set_=update_cols
                )

                session.execute(stmt)
            session.commit()
            print(f"Datos insertados o actualizados en {table.name}")
    except Exception as e:
        print(f"Error al cargar datos en la tabla {table.name} desde {csv_path}: {e}")

# Lista de tablas a cargar
tables_info = [
    (DimDate, "tablas/DimDate.csv", "Dateid"),
    (DimCustomerSegment, "tablas/DimCustomerSegment.csv", "Segmentid"),
    (DimProduct, "tablas/DimProduct.csv", "Productid"),
    (FactSales, "tablas/FactSales.csv", "Salesid"),
]

# Ejecutar la carga para cada tabla
for table, csv_path, pk in tables_info:
    try:
        upsert_from_csv(engine, table, csv_path, pk)
    except Exception as e:
        print(f"Falló la carga para la tabla {table.name}: {e}")

print("Proceso terminado.")



