FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    redis-server \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN playwright install-deps chromium && playwright install chromium

COPY . .

# Strip Windows line endings from all shell scripts and Python files
# (safety net in case source files are ever edited on Windows again)
RUN find /app -type f \( -name "*.sh" -o -name "*.py" \) -exec sed -i 's/\r$//' {} +

RUN chmod +x /app/entrypoint.sh

EXPOSE 5523

ENTRYPOINT ["/app/entrypoint.sh"]
