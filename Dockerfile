FROM apache/spark:4.0.0

USER root

# Install Python3 and pip (if not already present)
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* 

# (Optional) Ensure PySpark matches Spark version
RUN pip3 install pyspark

# Add Hadoop AWS and Snowflake connector jars
COPY artifacts/*.jar /opt/spark/jars/
RUN chown 185:185 /opt/spark/jars/*.jar && chmod 644 /opt/spark/jars/*.jar

# Create app directory and give ownership to Spark user
RUN mkdir -p /opt/spark/app && chown -R 185:185 /opt/spark/app

# Create /data for logs, owned by Spark user (not 777 world-writable)
RUN mkdir -p /data && chown 185:185 /data && chmod 775 /data

# Copy Spark applications
COPY sales_job.py /opt/spark/app/
COPY delivery_job.py /opt/spark/app/
COPY business_job.py /opt/spark/app/

WORKDIR /opt/spark/app

USER 185
