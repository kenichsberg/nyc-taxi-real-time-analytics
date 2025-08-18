FROM bitnami/spark:3.5.6

USER root
RUN apt-get update && apt-get install -y curl python3-pip python3-venv

RUN pip install uv

WORKDIR /app
COPY uv.lock stream-transformer/pyproject.toml /app/

RUN uv pip install --system .

COPY ./stream-transformer /app

RUN chmod +x /app/run-transformer.sh
ENTRYPOINT ["/app/run-transformer.sh"]
