# Multi-stage build for efficiency
FROM python:3.11-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

# Install runtime dependencies including Chrome
RUN apt-get update && apt-get install -y \
    # Basic utilities
    tzdata \
    cron \
    curl \
    vim \
    supervisor \
    sudo \
    wget \
    gnupg \
    unzip \
    # Chrome dependencies
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    libxss1 \
    xdg-utils \
    # Fonts for Chrome
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Install ChromeDriver
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d. -f1-3) \
    && CHROMEDRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION%%.*}") \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROMEDRIVER_VERSION}/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip \
    && unzip -j /tmp/chromedriver.zip "*/chromedriver" -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver \
    && rm /tmp/chromedriver.zip

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set timezone
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Create non-root user with sudo privileges
RUN useradd -m -s /bin/bash etluser && \
    echo "etluser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Create necessary directories with proper permissions
RUN mkdir -p /app /data /logs /config && \
    chown -R etluser:etluser /app /data /logs /config && \
    chmod -R 755 /data /logs /config

# Set working directory
WORKDIR /app

# Copy application files
COPY --chown=etluser:etluser *.py ./

# Copy entrypoint script
COPY --chown=etluser:etluser docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Copy supervisor configuration
COPY --chown=etluser:etluser supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Create cron job for daily ETL (6 AM)
RUN echo "0 6 * * * etluser cd /app && /opt/venv/bin/python fund_etl_scheduler.py --run-daily >> /logs/cron.log 2>&1" > /etc/cron.d/fund-etl && \
    chmod 0644 /etc/cron.d/fund-etl && \
    crontab -u etluser /etc/cron.d/fund-etl

# Expose port for health check endpoint
EXPOSE 8080

# Set entrypoint
ENTRYPOINT ["docker-entrypoint.sh"]

# Default command
CMD ["scheduler"]
