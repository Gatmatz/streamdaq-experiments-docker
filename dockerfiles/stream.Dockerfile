# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements/requirements_stream.txt ./requirements.txt

# Install system dependencies and Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the kafka producer source code
COPY stream/*.py .


CMD ["/bin/sh", "-c", "\
    echo The value of STREAM environment variable is: $STREAM; \
    if [ \"$STREAM\" = \"atlantis\" ]; then \
        echo 'Starting Atlantis stream generator script...'; \
        python atlantis_stream_generation.py; \
    else \
        echo 'Starting custom stream generator script...'; \
        python custom_stream_generation.py; \
    fi \
"]