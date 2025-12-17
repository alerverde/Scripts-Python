import os
from dotenv import load_dotenv
from extract import dataframes
from load import load_all
from schema import metadata, get_engine
from schema import DimDate, DimCustomerSegment, DimProduct, FactSales

# Cargar variables de entorno
load_dotenv()
RENDER_DB_URL = os.getenv("RENDER_DB_URL")

def main():
    """
    Función principal que ejecuta el pipeline ETL completo:
    1. Crea o verifica el esquema en la base destino.
    2. Inserta o actualiza datos extraídos previamente.
    """
    print("\nIniciando pipeline ETL...\n")
    
    # Crear motor de conexión y asegurarse que las tablas existen
    engine = get_engine(RENDER_DB_URL)
    metadata.create_all(engine)
    print("Esquema en la base espejo creado o verificado.\n")

    # Cargar los datos extraídos en la base destino
    load_all(dataframes)

    print("\nPipeline ETL completado exitosamente.")

if __name__ == "__main__":
    main()