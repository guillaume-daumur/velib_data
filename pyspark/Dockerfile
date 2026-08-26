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
COPY log4j2.properties .

# JAVA_TOOL_OPTIONS est lu par la JVM au tout premier démarrage, avant même
# que PySpark ne lance spark-submit — contrairement à spark.driver.extraJavaOptions
# (ignoré en local[2] : la JVM du driver est déjà lancée par py4j avant que
# Spark ne lise cette conf). C'est le seul moyen fiable de faire passer
# notre log4j2.properties ici.
ENV JAVA_TOOL_OPTIONS="-Dlog4j.configurationFile=file:/app/log4j2.properties"

# Créer les répertoires de données et checkpoints
# Ils seront écrasés par le volume Docker au lancement
RUN mkdir -p /app/data/raw /app/data/checkpoints

CMD ["python", "spark_streaming_job.py"]
