FROM python:3.9-slim

RUN groupadd -g 1000 appuser && \
    useradd -u 1000 -g appuser -s /bin/bash -m appuser

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* && \
    curl -LO "https://dl.k8s.io/release/$(curl -Ls https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    install -m 755 kubectl /usr/local/bin/kubectl

WORKDIR /app

COPY app/ app/
COPY rayworker_code/ rayworker_code/
COPY requirements.txt .

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

USER 1000

EXPOSE 8000

CMD ["python", "app/start.py"]