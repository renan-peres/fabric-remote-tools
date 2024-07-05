# Use a specific Python version base image from Docker Hub
FROM python:3.12.4

# Set environment variables to avoid issues with debconf
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies and clean up any other Python versions
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    gcc \
    g++ \
    unixodbc-dev \
    libssl-dev \
    libffi-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install python-dotenv to manage environment variables
RUN pip install python-dotenv

# Switch to vscode user
USER vscode
WORKDIR /workspace

# Install Python packages
COPY requirements.txt .
RUN pip3 --disable-pip-version-check --no-cache-dir install -r requirements.txt \
    && rm requirements.txt

# Copy set_env_vars.py to the container
COPY set_env_vars.py /workspace/set_env_vars.py

# Set the entrypoint to bash
CMD ["/bin/bash"]