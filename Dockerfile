FROM apache/airflow:2.9.2

# Switch to root to install packages
USER airflow

# Copy and install requirements
COPY requirements.txt /requirements.txt
RUN pip3 install --no-cache-dir -r /requirements.txt

# Switch back to airflow user
USER airflow
