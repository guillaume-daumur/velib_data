FROM python:3.11-slim

# procps fournit pgrep, utilisé par le healthcheck Docker (docker-compose.yml)
RUN apt-get update && \
    apt-get install -y --no-install-recommends procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py .
COPY health_checks.py .
COPY monitor.py .

CMD ["python", "monitor.py"]
