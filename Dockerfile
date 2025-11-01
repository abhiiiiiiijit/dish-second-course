# ============================================
# 1. Base image (Python 3.10)
# ============================================
FROM python:3.10-slim

# ============================================
# 2. Set working directory
# ============================================
WORKDIR /app

# ============================================
# 3. Environment variables
# ============================================
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VERSION=1.8.3
ENV POETRY_VIRTUALENVS_CREATE=false
ENV PATH="/root/.local/bin:$PATH"

# ============================================
# 4. Install system dependencies and Poetry
# ============================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ============================================
# 5. Copy project files
# ============================================
COPY pyproject.toml poetry.lock ./
COPY src ./src
COPY data ./data

# ============================================
# 6. Install dependencies with Poetry
# ============================================
RUN poetry install --no-root --no-interaction --no-ansi

# ============================================
# 7. Default environment variables (can be overridden)
# ============================================
ENV GCP_PROJECT=""
ENV GOOGLE_APPLICATION_CREDENTIALS=""
ENV API_KEY=""

# ============================================
# 8. Entry point to run the data pipeline
# ============================================
ENTRYPOINT ["python", "src/etl/data_pipeline.py"]
