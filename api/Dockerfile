FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p screenshots data

# Set Python path
ENV PYTHONPATH=/app
ENV TZ=UTC

# Expose port
EXPOSE 8001

# Run the API server
CMD ["python", "api_server_enhanced.py"]
