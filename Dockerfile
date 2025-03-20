# I used a newer Apache Airflow base image than original one specified in base installation.
FROM apache/airflow:latest-python3.9

# Copy only requirements, while leveraging Docker caching.
COPY requirements.txt /tmp/requirements.txt 

# Install dependencies. This step will be cached if requirements.txt hasn't changed.
RUN pip install -r /tmp/requirements.txt

# Copy the rest of the application files. This step will invalidate the cache only if app files change.
COPY . /opt/airflow/