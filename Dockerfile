FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py database.py create_schema.sql distributions.json \
     generate_synthetic_to_s3.py etl_to_clickhouse.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "etl_to_clickhouse.py"]
