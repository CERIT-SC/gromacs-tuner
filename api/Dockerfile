FROM python:3.13-slim

RUN groupadd -g 1000 appuser && \
    useradd -u 1000 -g appuser -s /bin/bash -m appuser

WORKDIR /app

COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && rm requirements.txt

COPY api/ api/
COPY migrations/ migrations/
COPY alembic.ini .

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1

RUN chown -R appuser:appuser /app

USER 1000

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/api/health').getcode() == 200 else 1)"

CMD ["python", "api/start.py"]
