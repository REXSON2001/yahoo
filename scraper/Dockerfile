FROM python:3.11-slim

# -------------------------------
# System dependencies for Chrome
# -------------------------------
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libasound2 \
    libxshmfence1 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Install Google Chrome ONLY
# -------------------------------
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb

# -------------------------------
# Python setup
# -------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install webdriver-manager

COPY . .

RUN mkdir -p screenshots data logs

# -------------------------------
# Environment
# -------------------------------
ENV CHROME_BIN=/usr/bin/google-chrome
ENV PYTHONPATH=/app

# -------------------------------
# Run scraper
# -------------------------------
CMD ["python", "yahoo_persistent_enhanced_complete.py"]
