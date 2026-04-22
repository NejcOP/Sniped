FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHON_ENV=production \
    APP_ENV=production \
    NODE_ENV=production \
    STATELESS_SUPABASE_ONLY=1 \
    SUPABASE_PRIMARY_DB=1 \
    MALLOC_ARENA_MAX=2 \
    LOG_LEVEL=warning \
    DB_POOL_SIZE=1 \
    DB_MAX_OVERFLOW=0 \
    APP_THREADPOOL_WORKERS=2 \
    SCHEDULER_MAX_WORKERS=1 \
    ENRICH_CONCURRENCY_LIMIT=2 \
    RUN_STARTUP_JOBS=0

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnss3 \
    libpango-1.0-0 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    libxshmfence1 \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install -r /app/backend/requirements.txt
RUN python -m playwright install chromium

COPY backend /app/backend
COPY pgdb.py /app/pgdb.py
RUN mkdir -p /app/profiles/maps_profile

EXPOSE 8000

CMD ["sh", "-c", "exec uvicorn backend.app:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --log-level ${LOG_LEVEL:-warning} --no-access-log --timeout-keep-alive 5 --timeout-graceful-shutdown 15"]