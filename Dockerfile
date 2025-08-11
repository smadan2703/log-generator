FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create log directory
RUN mkdir -p /var/log/app && chmod 755 /var/log/app

# Copy minimal requirements
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY log_generator.py .

# Expose port
EXPOSE 8080

# Volume for logs
VOLUME ["/var/log/app"]

# Environment variables
ENV LOG_DIR=/var/log/app
ENV PORT=8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Run ONLY with Python (NO gunicorn anywhere)
CMD ["python", "log_generator.py"]