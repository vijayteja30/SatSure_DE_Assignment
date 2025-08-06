FROM apache/airflow:2.9.0-python3.9


# Set working directory to AIRFLOW_HOME (best practice)
WORKDIR ${AIRFLOW_HOME}


# Copy requirements.txt before install
COPY requirements.txt .

# Switch to airflow user before pip install (as required by Airflow)
USER airflow

# Install Python packages for your DAGs
RUN pip install --no-cache-dir -r requirements.txt

# Set PYTHONPATH so Airflow can import your modules/
ENV PYTHONPATH="/opt/airflow"

# Switch back to root user if needed for airflow to run
USER root
