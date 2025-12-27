FROM selenium/standalone-chrome:latest

USER root

# -------------------------------
# Install Python 3.11 (NOT 3.12)
# -------------------------------
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    pkg-config \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3.11 /usr/bin/python

# -------------------------------
# App setup
# -------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p screenshots data logs

ENV PYTHONPATH=/app
ENV CHROME_BIN=/usr/bin/google-chrome

CMD ["python", "yahoo_persistent_enhanced_complete.py"]
