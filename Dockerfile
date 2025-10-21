FROM python:3.12

# Configure apt and install packages
RUN apt-get update \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
#    && apt-get -y install git iproute2 procps lsb-release \
RUN pip install --upgrade pip

COPY requirements.txt .
RUN pip --no-cache-dir install -r requirements.txt

# Set the working directory to /app
WORKDIR /app