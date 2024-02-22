FROM python:3.11-slim-bullseye

# Set working directory
WORKDIR /opt/app

# Install packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-launchpadlib \
    python3-dev \
    python3-pip \
    python3-venv \
    gcc \
    && apt-get clean

# Update pip
RUN pip install --no-cache-dir --upgrade pip

# Copy requirements.txt
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
