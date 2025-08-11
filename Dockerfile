FROM python:3.13-alpine3.22

WORKDIR /app

# Install build dependencies for Python packages
RUN apk add --no-cache \
    gcc \
    musl-dev \
    linux-headers \
    && rm -rf /var/cache/apk/*

# Copy requirements and install Python dependencies  
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY predictor.py .

# Create data directory for TLE caching
RUN mkdir -p /data && \
    adduser -D -u 1000 predictor && \
    chown -R predictor:predictor /app /data

USER predictor

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV CONFIG_PATH=/config/config.yml

CMD ["python", "predictor.py"]
