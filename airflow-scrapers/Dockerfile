FROM apache/airflow:2.7.2-python3.8

USER root

# Create logs directory and set permissions
RUN mkdir -p /opt/airflow/logs && \
  chown -R airflow:root /opt/airflow/logs && \
  chmod -R 775 /opt/airflow/logs


# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  libffi-dev \
  libpq-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
COPY constraints-python3.8.txt /constraints-python3.8.txt

# Upgrade pip and install requirements
RUN pip install --no-cache-dir --upgrade pip && \
  pip install --no-cache-dir -r /requirements.txt

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow

# Create directories with proper permissions
USER root
RUN mkdir -p ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/include ${AIRFLOW_HOME}/config && \
    chown -R airflow:root ${AIRFLOW_HOME}/dags ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/plugins ${AIRFLOW_HOME}/include ${AIRFLOW_HOME}/config && \
    chmod -R 777 ${AIRFLOW_HOME}/logs

USER airflow

# Copy plugins, dags, and include directories
COPY --chown=airflow:root plugins/ ${AIRFLOW_HOME}/plugins/
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root include/ ${AIRFLOW_HOME}/include/

# Add the include directory to PYTHONPATH
ENV PYTHONPATH=${PYTHONPATH}:${AIRFLOW_HOME}
