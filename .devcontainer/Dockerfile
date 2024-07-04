FROM mcr.microsoft.com/vscode/devcontainers/universal:2-linux

# Install additional packages
USER root
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        unixodbc-dev \
        openjdk-11-jdk \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/* \
    && id -u vscode >/dev/null 2>&1 || useradd -ms /bin/bash vscode

# Set environment variables
ENV HOME=/home/vscode \
    PATH="${HOME}/.local/bin:${PATH}" \
    SHELL=/bin/zsh \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    VIRTUAL_ENV=/opt/venv

# Switch to vscode user for subsequent operations
USER vscode
WORKDIR /workspace

# Set up Python virtual environment
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install Python packages
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip --disable-pip-version-check --no-cache-dir install -r requirements.txt && \
    pip install https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz && \
    python -m ipykernel install --user --name=python3 --display-name "Python 3 (latest)" && \
    rm requirements.txt

# Set up Oh My Zsh with Powerlevel10k theme
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k \
    && sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="powerlevel10k\/powerlevel10k"/g' ~/.zshrc \
    && echo 'POWERLEVEL9K_DISABLE_CONFIGURATION_WIZARD=true' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_LEFT_PROMPT_ELEMENTS=(dir vcs)' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS=(status root_indicator background_jobs time)' >> ~/.zshrc

# Copy set_env_vars.py to the container
COPY set_env_vars.py .

# Set the entrypoint to zsh to keep the container running
CMD ["zsh"]