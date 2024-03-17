FROM apache/airflow:2.8.2

COPY .env /.env
COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt



