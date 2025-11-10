FROM --platform=linux/amd64 apache/airflow:2.10.5

USER root

# Configurar mirrors de Debian y instalar dependencias del sistema
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bullseye-security main" >> /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bullseye-updates main" >> /etc/apt/sources.list && \
    apt-get clean && \
    apt-get update --fix-missing && \
    apt-get install -y --fix-missing \
    gcc \
    g++ \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

USER airflow

# Copiar requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt