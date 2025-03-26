FROM python:3.12-slim


# Disabled Prompt
ENV DEBIAN_FRONTEND=noninteractive

# Install cron and any other necessary packages
RUN apt-get update && apt-get install -y \
    libpq-dev \
    tzdata \
    gcc \
    postgresql-client-common \
    postgresql-common \
    curl \
    gnupg \
    lsb-release \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set Version GCLOUD SDK

ARG CLOUD_SDK_VERSION=496.0.0
# Unduh installer Google Cloud SDK secara langsung dari Google
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && tar -xzvf google-cloud-cli-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && ./google-cloud-sdk/install.sh --quiet \
    && rm google-cloud-cli-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz

# Set environment variables untuk gcloud
ENV PATH=$PATH:/google-cloud-sdk/bin

# Set the timezone to Asia/Jakarta
ENV TZ=Asia/Jakarta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install PDM
RUN pip install -U pdm

# COPY all Files project  
COPY pyproject.toml pdm.lock README.md .env /dbaproject/
COPY src/ /dbaproject/src

# SET WORKDIR PROJECT
WORKDIR /dbaproject


# LOCK PDM 
RUN pdm lock

# INSTALL all Packages Dependency
RUN pdm install --check --prod --no-editable


# CRONTAB
# COPY run.sh /usr/local/bin/
# RUN chmod +x /run.sh
# RUN run.sh

# FINAL COMMAND RUN CONTAINER
# ENTRYPOINT ['/usr/local/bin/run.sh']

# ENTRYPOINT ["pdm","list"]
ENTRYPOINT ["pdm","run","python","-m","backup_restore_engine"]
# ENTRYPOINT ["cron","-f", "-l", "2"]
