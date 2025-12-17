# load.py
import os
from dotenv import load_dotenv
from schema import DimDate, DimCustomerSegment, DimProduct, FactSales
from schema import get_engine
from sqlalchemy.orm import Session
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Cargar variables de entorno y establecer conexión con la base de datos en la nube
load_dotenv()
RENDER_DB_URL = os.getenv("RENDER_DB_URL")
engine = get_engine(RENDER_DB_URL)

def upsert_from_df(engine, table, df, id_column):
    """
    Inserta o actualiza registros en una tabla SQL a partir de un DataFrame.
    Si un registro con el mismo valor en la columna de ID ya existe,
    se actualizan sus columnas. Si no, se inserta un nuevo registro.

    Args:
        engine: SQLAlchemy engine para la conexión a la base de datos.
        table: Clase de tabla SQLAlchemy (por ejemplo, DimProduct).
        df: DataFrame de pandas con los datos a cargar.
        id_column: Nombre de la columna identificadora única (clave primaria).
    """
    with engine.begin() as connection:
        for _, row in df.iterrows():
            stmt = pg_insert(table).values(**row.to_dict())
            update_dict = {c.name: stmt.excluded[c.name] for c in table.columns if c.name != id_column}
            stmt = stmt.on_conflict_do_update(index_elements=[id_column], set_=update_dict)
            connection.execute(stmt)

def load_all(dataframes):
    """
    Carga todos los DataFrames en sus respectivas tablas en la base de datos.
    Realiza una operación de UPSERT para evitar duplicados y mantener actualizados los datos.
    """
    upsert_from_df(engine, DimDate, dataframes["DimDate"], "dateid")
    upsert_from_df(engine, DimCustomerSegment, dataframes["DimCustomerSegment"], "Segmentid")
    upsert_from_df(engine, DimProduct, dataframes["DimProduct"], "Productid")
    upsert_from_df(engine, FactSales, dataframes["FactSales"], "Salesid")
