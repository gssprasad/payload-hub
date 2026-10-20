# Payload Hub - one image for the backend (uvicorn) and the web console (Streamlit).
#   docker build -t payload-hub .
# Debian trixie brings PostgreSQL 17 client tools, so backups (pg_dump) work against PostgreSQL 17 and older.
FROM python:3.13-slim-trixie

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app ./app
COPY ui ./ui
COPY migrations ./migrations
COPY samples ./samples
COPY scripts ./scripts
COPY alembic.ini streamlit_app.py README.md ./
COPY docs ./docs

# data/: the master key (if not given as PAYLOAD_HUB_SECRET_KEY), backups and polled folders - mount a volume here
RUN useradd --create-home --uid 10001 hub && mkdir -p /app/data && chown -R hub /app/data
USER hub
VOLUME ["/app/data"]
EXPOSE 8000 8501

ENTRYPOINT ["tini", "--"]
# default: the backend; the web console overrides the command (see docker-compose.yml)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=4 \
  CMD python -c "import urllib.request,sys; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4)" || exit 1
