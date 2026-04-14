FROM quay.io/astronomer/astro-runtime:12.1.1

COPY scripts/ /opt/airflow/scripts/

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
