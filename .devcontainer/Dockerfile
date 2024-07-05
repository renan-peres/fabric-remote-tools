# Python base image from Docker Hub
FROM python:3.12.4

# Install system dependencies
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc-dev \
    ca-certificates \
    curl \
    apt-transport-https \
    lsb-release \
    gnupg \
    && curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update && apt-get install -y azure-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up workspace and install Python Packages
WORKDIR /workspace
COPY requirements.txt set_env_vars.py ./

# Upgrade pip
RUN pip install --upgrade pip
    
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz \
    && pip install python-dotenv ipykernel \
    && python -m ipykernel install --user --name=python3

CMD ["bash"]