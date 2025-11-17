# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements/requirements_output_consumer.txt ./requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer source code
COPY output_consumer/ .

# Command to run the appropriate consumer script based on STREAM environment variable
CMD ["/bin/sh", "-c", "\
    echo The value of STREAM environment variable is: $STREAM; \
    if [ \"$STREAM\" = \"atlantis\" ]; then \
        echo 'Running atlantis output consumer...'; \
        python atlantis_output_consumer_postgres.py; \
    elif [ \"$STREAM\" = \"stable\" ]; then \
        echo 'Running stable output consumer...'; \
        python stable_output_consumer_postgres.py; \
    else \
        echo 'STREAM not set to atlantis or stable — defaulting to pathway output consumer...'; \
        python pathway_output_consumer_postgres.py; \
    fi \
"]
