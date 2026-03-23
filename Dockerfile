# Playwright + Chromium preinstalled — keep tag in sync with `playwright` on PyPI when you bump it.
# Tags: https://playwright.dev/python/docs/docker
FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MANUAL_SCRAPE_HEADLESS=1

WORKDIR /app

COPY requirements.txt .
# Base image browsers match a specific Playwright release — pin to the same minor.
RUN pip install -U pip wheel \
    && pip install -r requirements.txt gunicorn \
    && pip install "playwright==1.49.1"

COPY . .

EXPOSE 8000

# Override in compose for workers
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:8000", "--timeout", "120", "app:app"]
