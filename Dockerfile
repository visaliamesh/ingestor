# Visalia Mesh ingestor. Connects to a radio and POSTs to the dashboard's
# /api ingest routes. Configure with env vars (see .env.example).
FROM python:3.12-slim

WORKDIR /ingestor
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestor.py .

# Set INSTANCE_DOMAIN, API_TOKEN, CONNECTION, PROTOCOL at run time.
CMD ["python", "ingestor.py"]
