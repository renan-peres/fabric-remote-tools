FROM mcr.microsoft.com/devcontainers/python:3.10

# Install additional packages
USER root
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        unixodbc-dev \
        openjdk-11-jdk \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch to vscode user
USER vscode
WORKDIR /workspace

# Install Python packages
COPY requirements.txt .
RUN pip3 --disable-pip-version-check --no-cache-dir install -r requirements.txt \
    && rm requirements.txt

# Copy set_env_vars.py to the container
COPY set_env_vars.py .

# Set the entrypoint to bash to keep the container running
CMD ["bash"]