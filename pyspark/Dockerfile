FROM python:3.11-slim

# Java est requis pour exécuter Spark (PySpark n'inclut pas de JVM)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        default-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_streaming_job.py .

# Créer les répertoires de données et checkpoints
# Ils seront écrasés par le volume Docker au lancement
RUN mkdir -p /app/data/raw /app/data/checkpoints

CMD ["python", "spark_streaming_job.py"]
