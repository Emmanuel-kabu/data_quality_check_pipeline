# ============================================================================
# Data Quality Governance Pipeline — Dockerfile
# ============================================================================
# Multi-stage build for the pipeline container.
# Runs as non-root user in production.
# ============================================================================

FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        && \
    rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Dependencies stage
# ---------------------------------------------------------------------------
FROM base AS dependencies

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Application stage
# ---------------------------------------------------------------------------
FROM dependencies AS app

# Create non-root user
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

# Copy application code
COPY src/ ./src/
COPY main.py .
COPY data/ ./data/

# Create output directories
RUN mkdir -p /app/reports /app/data/quarantine /app/data/versions \
             /app/data/review /app/reports/metrics && \
    chown -R pipeline:pipeline /app

# Switch to non-root user
USER pipeline

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import src.config; print('healthy')" || exit 1

# Default command
ENTRYPOINT ["python", "main.py"]
CMD ["--input", "data/customers_raw.csv", "--force"]
