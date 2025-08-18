FROM bitnami/spark:3.5.6

USER root
RUN apt-get update && apt-get install -y curl python3-pip python3-venv

RUN pip install uv

WORKDIR /app
COPY uv.lock simulated-gps-producer/pyproject.toml /app/

RUN uv pip install --system .

COPY ./simulated-gps-producer /app

RUN chmod +x /app/run-producer.sh
ENTRYPOINT ["/app/run-producer.sh"]
