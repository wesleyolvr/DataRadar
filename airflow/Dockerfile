FROM astrocrpublic.azurecr.io/runtime:3.1-14

COPY scripts/ /opt/airflow/scripts/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
