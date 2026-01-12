# FastAPI + Motor app
FROM python:3.11-slim

# Avoid Python buffering and .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps (optional: for faster CSV handling, locales)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Default environment (can be overridden by docker-compose / .env)
ENV SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8000

EXPOSE 8000

# Run the API with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
