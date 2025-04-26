FROM spark:3.5.5-java17-python3

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl \
      vim \
      ivy && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade --no-cache-dir pip && \
    python3 -m pip install --no-cache-dir pipenv

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}
WORKDIR $SPARK_HOME/work-dir

COPY Pipfile* .
RUN pipenv sync --dev --system --extra-pip-args="--prefer-binary"

ENV PYSPARK_PYTHON python3
ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
ENV PATH="/opt/spark/sbin:/opt/spark/bin:/usr/share/java:${PATH}"

ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077

COPY spark-defaults.conf "$SPARK_HOME/conf/"

COPY ivy.xml ./ivy.xml
RUN java -jar /usr/share/java/ivy.jar -retrieve "/opt/spark/jars/[artifact]-[revision].[ext]"

COPY entrypoint.sh .
# USER spark
