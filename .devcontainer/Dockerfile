FROM mcr.microsoft.com/vscode/devcontainers/universal:2-linux

# Install additional packages
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
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k \
    && sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="powerlevel10k\/powerlevel10k"/g' ~/.zshrc

# Set up a basic Powerlevel10k configuration
RUN echo 'POWERLEVEL9K_DISABLE_CONFIGURATION_WIZARD=true' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_LEFT_PROMPT_ELEMENTS=(dir vcs)' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS=(status root_indicator background_jobs time)' >> ~/.zshrc

# Set default shell to zsh
ENV SHELL /bin/zsh

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
