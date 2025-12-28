FROM python:3.11-slim

# Install system dependencies for Chrome - FIXED
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    unzip \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome (stable version)
RUN wget -q -O /tmp/chrome.deb "https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb" \
    && apt-get update \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb

# Install ChromeDriver with FIXED version extraction
RUN CHROME_VERSION=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+\.\d+' | head -1) \
    && echo "Chrome version: $CHROME_VERSION" \
    && MAJOR_VERSION=$(echo $CHROME_VERSION | cut -d. -f1) \
    && echo "Major version: $MAJOR_VERSION" \
    && wget -q -O /tmp/versions.json "https://googlechromelabs.github.io/chrome-for-testing/last-known-good-versions-with-downloads.json" \
    && LATEST_VERSION=$(cat /tmp/versions.json | grep -o '"version":"[^"]*"' | head -1 | cut -d'"' -f4) \
    && echo "Latest stable ChromeDriver version: $LATEST_VERSION" \
    && wget -q -O /tmp/chromedriver.zip "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/$LATEST_VERSION/linux64/chromedriver-linux64.zip" \
    && unzip /tmp/chromedriver.zip -d /tmp/ \
    && mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf /tmp/chromedriver* /tmp/versions.json

WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p screenshots data logs

# Set environment variables
ENV CHROME_BIN=/usr/bin/google-chrome
ENV CHROMEDRIVER_PATH=/usr/local/bin/chromedriver
ENV DISPLAY=:99
ENV PYTHONPATH=/app
ENV TZ=UTC

# Run the scraper
CMD ["python", "yahoo_persistent_enhanced_complete.py"]
