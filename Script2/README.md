# Script 2 -Extracción incremental desde API externa (BCRA)

En este script se automatiza la extracción de cotizaciones del dólar tipo vendedor desde la página oficial del Banco Central de la República Argentina (BCRA) mediante web scraping con Selenium, y luego se cargan los datos en una base de datos PostgreSQL alojada en Render.

### Ingesta incremental 

El script `main.py` ejecuta un pipeline ETL automatizado para extraer la cotización del dólar (tipo vendedor) desde el sitio del BCRA, procesar los datos y cargarlos en una base de datos PostgreSQL alojada en Render.

El proceso consta de:
1. Extracción mediante Selenium de la tabla HTML de cotizaciones desde el sitio web oficial del BCRA.
2. Transformación de los datos en un DataFrame de Pandas con formato limpio.
3. Carga incremental en la tabla `cotizaciones` en la base espejo en la nube (Render), evitando duplicados.


### Automatización
Para automatizar el pipeline el cronjob configurado es:

0 0 * * 5 /usr/bin/python3 /ruta/completa/a/Script2/main.py >> /ruta/completa/a/Script2/logs/bnra_update.log 2>&1

Esto ejecutará el script main.py ubicado en la carpeta Script2 todos los días a la medianoche (00:00 hs). La salida estándar y los errores se guardarán en el archivo etl.log dentro de la misma carpeta Script2/logs/. Nota: La carpeta logs debe existir previamente para que el archivo de log pueda ser creado correctamente.

Importante: En este caso la opción preferida hubiese sido GitHub Actions, pero la configuración de los drivers de Firefox y el acceso a la web durante la ejecución en GitHub no es sencilla. Una alternativa posible sería probar con Chrome.
El .yml se encuentra en la carpeta .github/workflows

```yaml
name: BNRA Update

on:
  workflow_dispatch:
  schedule:
    - cron: '0 0 * * 5'

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Firefox via Snap
        run: |
          sudo snap install firefox

      - name: Install Geckodriver manually
        run: |
          GECKO_VERSION=$(curl -s https://api.github.com/repos/mozilla/geckodriver/releases/latest  \ | grep tag_name | cut -d '"' -f4)
          wget https://github.com/mozilla/geckodriver/releases/download/$GECKO_VERSION/geckodriver-$GECKO_VERSION-linux64.tar.gz
          tar -xvzf geckodriver-*.tar.gz
          chmod +x geckodriver
          sudo mv geckodriver /usr/local/bin/

      - name: Install Xvfb and Python dependencies
        run: |
          sudo apt-get update
          sudo apt-get install -y xvfb
          pip install selenium beautifulsoup4 python-dotenv pandas sqlalchemy psycopg2-binary

      - name: Print Firefox location
        run: |
          which firefox
          firefox --version

      - name: Run scraper with Xvfb (headless Firefox)
        run: |
          xvfb-run -a python Script2/main.py
        env:
          ORIGIN_DB_URL: ${{ secrets.ORIGIN_DB_URL }}
```

