FROM openjdk:8-jdk-alpine
RUN apk add --no-cache bash

# Install Python 3
RUN apk add --no-cache python3 py3-pip

# Install Spark
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=2.7
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar xvz -C /opt/
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Iceberg support, COPY due to issue with docker wger repo1.maven, not worth investing time to ivestigate
COPY iceberg-spark3-runtime-0.12.1.jar /opt/spark/jars/

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /opt/spark

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

COPY log4j.properties /opt/spark/conf/log4j.properties

ENTRYPOINT ["/entrypoint.sh"]
