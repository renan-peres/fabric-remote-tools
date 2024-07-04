FROM mcr.microsoft.com/vscode/devcontainers/universal:2-linux

# Install additional packages
USER root
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    unixodbc-dev \
    openjdk-11-jdk \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Create vscode user if it doesn't exist
RUN id -u vscode >/dev/null 2>&1 || useradd -ms /bin/bash vscode

# Set up environment for vscode user
ENV HOME=/home/vscode
ENV PATH="${HOME}/.local/bin:${PATH}"
WORKDIR ${HOME}

WORKDIR /workspace

# Copy requirements.txt and set_env_vars.py to the container
COPY requirements.txt .
COPY set_env_vars.py .

# Switch to vscode user for subsequent operations
USER vscode

# Install Python packages
RUN pip3 --disable-pip-version-check --no-cache-dir install -r requirements.txt \
    && rm requirements.txt

# Install fabric_remote_tools from GitHub
RUN pip3 --disable-pip-version-check --no-cache-dir install \
    https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz

# Set up IPython kernel (latest version)
RUN python -m ipykernel install --user --name=python3 --display-name "Python 3 (latest)"

# Set up Oh My Zsh with Powerlevel10k theme
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

# Set the entrypoint to bash to keep the container running
CMD ["zsh"]