FROM selenium/standalone-chrome:latest

USER root

# -------------------------------
# Install Python
# -------------------------------
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# App setup
# -------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p screenshots data logs

# -------------------------------
# Environment
# -------------------------------
ENV PYTHONPATH=/app
ENV CHROME_BIN=/usr/bin/google-chrome

# -------------------------------
# Run scraper
# -------------------------------
CMD ["python3", "yahoo_persistent_enhanced_complete.py"]
