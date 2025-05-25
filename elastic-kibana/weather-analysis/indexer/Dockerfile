FROM python:3.10-slim

# Java for PySpark
# Install Java 17 headless JRE for PySpark
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY watch_and_index.py .

CMD ["python", "watch_and_index.py"]
