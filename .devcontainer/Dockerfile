FROM mcr.microsoft.com/msfabric/synapsevscode/fabric-synapse-vscode:latest

# Switch to root to install additional packages
USER root

# Install additional packages if needed
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        unixodbc-dev \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV HOME=/home/vscode \
    PATH="${HOME}/.local/bin:${PATH}" \
    SHELL=/bin/bash

# Switch back to vscode user
USER vscode
WORKDIR /workspace

# Install additional Python packages if needed
COPY requirements.txt .
RUN pip3 --disable-pip-version-check --no-cache-dir install -r requirements.txt \
    && rm requirements.txt

# Copy set_env_vars.py to the container
COPY set_env_vars.py .

# Set the entrypoint to bash to keep the container running
CMD ["bash"]