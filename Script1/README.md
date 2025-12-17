# Ejercicio 1 - Replicación de base de datos
El objetivo principal de este ejercicio es replicar datos de una base PostgreSQL local a una base en la nube (Render) mediante un pipeline ETL automatizado.

### Esquema de la base de datos

- `DimDate`: tabla de fechas con columnas para año, mes, día, etc.  
- `DimCustomerSegment`: segmentos de clientes con ciudades.  
- `DimProduct`: productos y tipos.  
- `FactSales`: tabla de hechos de ventas que referencia a las dimensiones.  

### Datos de origen

Los datos de origen están almacenados en archivos CSV dentro de la carpeta `/tablas`. Estos datos se cargan en la base local inicialmente con `crear_db.py`. El esquema se encuentra en `schema.py` y desde ahi se importa a `crear_db.py` y `load.py`.

### Pipeline ETL

- `extract.py`: se conecta a la base local y extrae los datos de las tablas a pandas DataFrames.  
- `load.py`: realiza el upsert (insert/update) en la base destino en Render para mantener la base espejo sincronizada.  
- `main.py`: orquesta la extracción y carga de datos.  

### Cómo correr localmente

1. Levantar un contenedor PostgreSQL local con Docker.
2. Crear base de datos localmente `python3 crear_db.py` (única vez). 
3. Para la automatización, se creó un cronjob local, ya que GitHub Actions no puede acceder a la base de datos local (localhost) directamente y exponerla (por ejemplo, con ngrok) no era viable ni requerido en la consigna.

El cronjob configurado es:

0 0 * * * /usr/bin/python3 /ruta/completa/a/Ejercicio1/main.py >> /ruta/completa/a/Ejercicio1/logs/etl.log 2>&1

Esto ejecutará el script `main.py` ubicado en la carpeta Ejercicio1 todos los días a la medianoche (00:00 hs). La salida estándar y los errores se guardarán en el archivo etl.log dentro de la misma carpeta Ejercicio1/logs/.
Nota: La carpeta logs debe existir previamente para que el archivo de log pueda ser creado correctamente.

### Cómo acceder a la base espejo
Esta disponible en https://dashboard.render.com/d/dpg-d28ahbbipnbc739hcfg0-a
