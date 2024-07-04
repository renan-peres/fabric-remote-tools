FROM mcr.microsoft.com/vscode/devcontainers/universal:2-linux

# Install additional packages
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    unixodbc-dev \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

USER vscode

# Install Python packages
RUN pip3 --disable-pip-version-check --no-cache-dir install \
    python-dotenv==1.0.1 \
    typing==3.7.4.3 \
    pandas==2.2.2 \
    pyodbc==5.1.0

# Install fabric_remote_tools from GitHub
RUN pip3 --disable-pip-version-check --no-cache-dir install \
    https://github.com/renan-peres/fabric-remote-tools/raw/main/fabric_remote_tools-0.1.1.tar.gz

# Install Synapse CLI
RUN curl https://raw.githubusercontent.com/microsoft/SynapseVSCode/main/install-synapse-cli.sh | bash

# Set up Oh My Zsh with Powerlevel10k theme
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" "" --unattended \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k \
    && sed -i 's/ZSH_THEME="robbyrussell"/ZSH_THEME="powerlevel10k\/powerlevel10k"/g' ~/.zshrc

# Set up a basic Powerlevel10k configuration
RUN echo 'POWERLEVEL9K_DISABLE_CONFIGURATION_WIZARD=true' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_LEFT_PROMPT_ELEMENTS=(dir vcs)' >> ~/.zshrc \
    && echo 'POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS=(status root_indicator background_jobs time)' >> ~/.zshrc

WORKDIR /workspace

# Set default shell to zsh
ENV SHELL /bin/zsh