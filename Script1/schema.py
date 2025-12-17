# schema.py
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, Date, ForeignKey
)
from sqlalchemy import create_engine

# Metadata global que agrupa todas las definiciones de tablas
metadata = MetaData()

DimDate = Table(
    "DimDate", metadata,
    Column("dateid", Integer, primary_key=True),
    Column("date", Date, nullable=False),
    Column("Year", Integer, nullable=False),
    Column("Quarter", Integer, nullable=False),
    Column("QuarterName", String(50), nullable=False),
    Column("Month", Integer, nullable=False),
    Column("Monthname", String(50), nullable=False),    
    Column("Day", Integer, nullable=False),
    Column("Weekday", Integer, nullable=False),
    Column("WeekdayName", String(50), nullable=False)
)

DimCustomerSegment = Table(
    "DimCustomerSegment", metadata,
    Column("Segmentid", Integer, primary_key=True),
    Column("City", String(50), nullable=False)
)

DimProduct = Table(
    "DimProduct", metadata,
    Column("Productid", Integer, primary_key=True),
    Column("Producttype", String(100), nullable=False)
)

FactSales = Table(
    "FactSales", metadata,
    Column("Salesid", String(100), primary_key=True),
    Column("Dateid", Integer, ForeignKey("DimDate.dateid"), nullable=False),
    Column("Productid", Integer, ForeignKey("DimProduct.Productid"), nullable=False),
    Column("Segmentid", Integer, ForeignKey("DimCustomerSegment.Segmentid"), nullable=False),
    Column("Price_PerUnit", Float, nullable=False),
    Column("QuantitySold", Integer, nullable=False),
)

def get_engine(db_url: str, ssl_required: bool = False):
    """
    Crea y devuelve un motor de conexión SQLAlchemy.

    Args:
        db_url (str): URL de conexión a la base de datos.
        ssl_required (bool): Indica si se debe usar SSL (por defecto False).

    Returns:
        sqlalchemy.engine.Engine: motor de conexión configurado.
    """
    if ssl_required:
        return create_engine(db_url, connect_args={"sslmode": "require"})
    return create_engine(db_url)

