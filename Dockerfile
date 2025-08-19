FROM apache/spark:4.0.0

# Switch to root to install packages & adjust permissions
USER root

RUN echo "Installing packages....."

# Install Python3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/* 

# Install PySpark
RUN pip3 install pyspark

# Add Hadoop AWS and Snowflake connector jars
COPY hadoop-aws-3.4.1.jar /opt/spark/jars/
COPY bundle-2.32.7.jar /opt/spark/jars/
COPY snowflake-jdbc-3.13.21.jar /opt/spark/jars/
COPY spark-snowflake_2.13-3.1.1.jar /opt/spark/jars/

# Fix jar permissions for Spark user
RUN chown 185:185 /opt/spark/jars/*.jar \
    && chmod 644 /opt/spark/jars/*.jar

# Create artifacts folder with open write permission
RUN mkdir -p /opt/spark/app/artifacts \
    && chmod 1777 /opt/spark/app/artifacts

# Create /data directory for log output and grant write access to Spark user
RUN mkdir -p /data \
    && chown 185:185 /data \
    && chmod 755 /data

# Copy Spark applications
COPY sales_job.py /opt/spark/app/sales_job.py
COPY delivery_job.py /opt/spark/app/delivery_job.py
COPY business_job.py /opt/spark/app/business_job.py

# Ensure Spark app directory exists and owned by Spark user
RUN mkdir -p /opt/spark/app \
    && chown -R 185:185 /opt/spark/app

RUN echo "Finish Jars"

# Set working directory
WORKDIR /opt/spark/app

RUN echo "Done"

# Switch to Spark default user
USER 185
