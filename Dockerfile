# Builder installs dependencies into a venv. The runtime image copies that venv
# and the application files, and leaves pip, wheels, and build caches behind.
FROM python:3.12-slim AS builder

WORKDIR /build
ENV PYTHONDONTWRITEBYTECODE=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --upgrade pip && pip install --no-cache-dir .

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    DATA_DIR=/data \
    MARKETS_FILE=/app/config/markets.json \
    STORAGE_BACKEND=local
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data \
    && chown app:app /data
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY config ./config
COPY sql ./sql
USER app
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health')"
CMD ["uvicorn", "airbnb_etl.api:app", "--host", "0.0.0.0", "--port", "8000"]
