# main.py
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, NUMERIC, select, func
import pandas as pd
from bs4 import BeautifulSoup
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

def get_max_date_from_db():
    """
    Consulta la última fecha cargada en la tabla cotizaciones de PostgreSQL.
    Returns:
        datetime.date or None: última fecha cargada o None si tabla vacía.
    """
    load_dotenv()
    RENDER_DB_URL = os.getenv("RENDER_DB_URL")
    engine = create_engine(RENDER_DB_URL, connect_args={"sslmode": "require"})
    metadata = MetaData()
    cotizaciones = Table(
        "cotizaciones", metadata,
        Column("fecha", Date, nullable=False),
        Column("moneda", String(50), nullable=False),
        Column("tipo_cambio", NUMERIC(10,4), nullable=False),
        Column("fuente", String(50), nullable=False)
    )
    with engine.connect() as conn:
        result = conn.execute(select(func.max(cotizaciones.c.fecha)))
        max_fecha = result.scalar()
    return max_fecha

def extract_dolar_bcra(fecha_desde_str, fecha_hasta_str):
    """
    Extrae datos históricos de la cotización del dólar tipo vendedor desde el sitio del BCRA
    utilizando Selenium y los convierte a un DataFrame de Pandas.
    Args:
        fecha_desde_str (str): Fecha inicio en formato 'YYYY-MM-DD'
        fecha_hasta_str (str): Fecha fin en formato 'YYYY-MM-DD'
    Returns:
        pd.DataFrame: DataFrame con columnas ['fecha', 'tipo_cambio']
    """

    print(f"Extrayendo cotizaciones del BCRA desde {fecha_desde_str} hasta {fecha_hasta_str}...")

    options = Options()
    options.headless = True
    driver = webdriver.Firefox(options=options)

    try:
        url = "https://www.bcra.gob.ar/PublicacionesEstadisticas/Principales_variables_datos.asp?serie=7927"
        driver.get(url)

        # Esperar a que cargue el formulario de fechas
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.NAME, "fecha_desde"))
        )

        # Completar fechas de consulta dinámicamente
        fecha_desde = driver.find_element(By.NAME, "fecha_desde")
        fecha_hasta = driver.find_element(By.NAME, "fecha_hasta")
        fecha_desde.clear()
        fecha_desde.send_keys(fecha_desde_str)
        fecha_hasta.clear()
        fecha_hasta.send_keys(fecha_hasta_str)

        # Hacer clic en el botón Consultar
        consultar_btn = driver.find_element(By.NAME, "B1")
        consultar_btn.click()

        # Esperar a que cargue la tabla de resultados
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table"))
        )
        time.sleep(3)  # Espera extra para asegurar carga completa

        # Extraer HTML de la tabla       
        soup = BeautifulSoup(driver.page_source, "html.parser")
        table = soup.find("table", {"class": "table"})

        data = []
        for tbody in table.find_all("tbody"):
            for row in tbody.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    fecha = cols[0].text.strip()
                    valor = cols[1].text.strip().replace(".", "").replace(",", ".")
                    try:
                        data.append({"fecha": fecha, "tipo_cambio": float(valor)})
                    except:
                        continue

        df = pd.DataFrame(data)
        df["fecha"] = pd.to_datetime(df["fecha"], dayfirst=True)
        print(f"Filas extraídas: {len(df)}")
        return df

    finally:
        driver.quit()

def load_to_render(df):
    """
    Carga los datos al esquema `cotizaciones` de la base PostgreSQL en Render.
    Realiza ingesta incremental en base a la última fecha registrada.

    Args:
        df (pd.DataFrame): DataFrame con las columnas ['fecha', 'tipo_cambio']
    """

    print("Cargando datos a PostgreSQL Render...")
    load_dotenv()
    RENDER_DB_URL = os.getenv("RENDER_DB_URL")
    engine = create_engine(RENDER_DB_URL, connect_args={"sslmode": "require"})
    metadata = MetaData()

    # Definición de la tabla (esquema espejo)
    cotizaciones = Table(
        "cotizaciones", metadata,
        Column("fecha", Date, nullable=False),
        Column("moneda", String(50), nullable=False),
        Column("tipo_cambio", NUMERIC(10,4), nullable=False),
        Column("fuente", String(50), nullable=False)
    )

    metadata.create_all(engine)

    df["moneda"] = "Dólar"
    df["fuente"] = "BCRA"
    df = df[["fecha", "moneda", "tipo_cambio", "fuente"]]

    with engine.connect() as conn:
        result = conn.execute(select(func.max(cotizaciones.c.fecha)))
        max_fecha = result.scalar()

    if max_fecha:
        max_fecha = pd.to_datetime(max_fecha)
        df = df[df["fecha"] > max_fecha]
        print(f"Ingestando incremental desde: {max_fecha.date() + pd.Timedelta(days=1)}")
    else:
        print("Ingesta completa inicial.")

    if not df.empty:
        with engine.begin() as conn:
            conn.execute(cotizaciones.insert(), df.to_dict(orient="records"))
        print(f"{len(df)} filas insertadas en 'cotizaciones'.")
    else:
        print("No hay nuevas filas para insertar.")


if __name__ == "__main__":
    try:
        max_fecha = get_max_date_from_db()
        if max_fecha:
            fecha_desde = (pd.to_datetime(max_fecha) + timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"Extrayendo datos desde: {fecha_desde}")
        else:
            fecha_desde = "2010-06-01"
            print("No hay datos previos, extrayendo desde 2010-06-01")

        fecha_hasta = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Extrayendo hasta: {fecha_hasta}")

        df = extract_dolar_bcra(fecha_desde, fecha_hasta)
        load_to_render(df)
    except Exception as e:
        print("Error general en el pipeline:")
        print(e)

