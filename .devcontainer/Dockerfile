# Stage 1: Python environment
FROM python:3.12.4 AS python_env

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

# Create a working directory
WORKDIR /app

# Copy requirements.txt and set_env_vars.py to the container
COPY requirements.txt .
COPY set_env_vars.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set up IPython kernel with a specific display name
RUN pip install ipykernel && python -m ipykernel install --user --name=python3 --display-name "Python 3.12.4"

# Install fabric_remote_tools
RUN pip install https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz

# Stage 2: Synapse environment
FROM mcr.microsoft.com/msfabric/synapsevscode/fabric-synapse-vscode:v2.0.6 AS synapse_env

# Install additional packages
RUN tdnf install -y gawk wget tar

# Setup SSH with secure root login
RUN tdnf update -y && \
    tdnf install -y openssh-server nc && \
    mkdir /var/run/sshd && \
    echo 'root:password' | chpasswd && \
    sed -i 's/\#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Install PySpark in the base Conda environment
ENV SPARK_VERSION=3.4.1
RUN conda install -c conda-forge pyspark==$SPARK_VERSION

# Generate new host keys
RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    chmod 600 /etc/ssh/ssh_host_rsa_key

# Final Stage: Combine environments
FROM synapse_env

# Copy Python environment from the first stage
COPY --from=python_env /usr/local /usr/local
COPY --from=python_env /app /app

# Set working directory
WORKDIR /app

# Start SSH daemon and keep container running
CMD ["/bin/sh", "-c", "/usr/sbin/sshd -D"]
