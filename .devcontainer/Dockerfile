FROM python:3.11-slim-bullseye

# Set working directory
WORKDIR /opt/app

# Update pip
RUN pip install --no-cache-dir --upgrade pip

# Copy requirements.txt
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
