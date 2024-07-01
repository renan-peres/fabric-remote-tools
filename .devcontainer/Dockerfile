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
    && rm -rf /var/lib/apt/lists/* \
    && apt-get remove -y python3.11 \
    && apt-get autoremove -y

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Install python-dotenv to manage environment variables
RUN pip install python-dotenv

# Create a working directory
WORKDIR /app

# Copy requirements.txt and set_env_vars.py to the container
COPY requirements.txt .
COPY set_env_vars.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set up IPython kernel
RUN pip install ipykernel && python -m ipykernel install --user --name=python3

# Install fabric_remote_tools
RUN pip install https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz

# Set the entrypoint to bash to keep the container running
CMD ["bash"]
