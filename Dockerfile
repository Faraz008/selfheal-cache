FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends iptables \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml .
COPY node ./node
COPY client ./client
RUN pip install --no-cache-dir .
ENTRYPOINT ["python", "-m", "node"]