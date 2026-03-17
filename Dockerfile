FROM apache/airflow:3.0.0

# Install Postgres provider, psycopg2 , huggingface SDK
COPY reqs.txt .
RUN pip install --no-cache-dir -r reqs.txt