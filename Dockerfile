FROM selenium/standalone-chrome:latest

WORKDIR /app

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create and activate virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip in the venv
RUN /opt/venv/bin/python -m pip install --upgrade pip

# Copy requirements and install
COPY requirements.txt .
RUN /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# Copy application code (including .env)
COPY . .

# Create logs directory
RUN mkdir -p /app/logs && chown -R seluser:seluser /app

# Create startup script
RUN echo '#!/bin/bash\n\
# Start Selenium in background\n\
/opt/bin/entry_point.sh &\n\
\n\
# Wait for Selenium to be ready\n\
echo "Waiting for Selenium to start..."\n\
for i in {1..30}; do\n\
  if curl -s http://localhost:4444/wd/hub/status > /dev/null 2>&1; then\n\
    echo "Selenium is ready!"\n\
    break\n\
  fi\n\
  echo "Still waiting... ($i/30)"\n\
  sleep 2\n\
done\n\
\n\
# Run the Python script\n\
cd /app\n\
/opt/venv/bin/python main.py\n\
\n\
# Keep container running to allow manual interaction if needed\n\
wait' > /app/start.sh && chmod +x /app/start.sh

# Switch back to seluser
USER seluser

# Expose VNC ports (Selenium image has built-in VNC)
EXPOSE 4444 5900 7900

CMD ["/app/start.sh"]

#docker run -it --rm --shm-size=2g -p 4444:4444 -p 5900:5900 -p 7900:7900 etlsssz