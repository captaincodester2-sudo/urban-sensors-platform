FROM apache/spark:4.2.1-python3
COPY spark_job.py /opt/app/spark_job.py
ENTRYPOINT [ "/opt/spark/bin/spark-submit", \
    "--master", "local[2]", \
    "--driver-memory", "1g", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.1", \
    "--conf", "spark.jars.ivy=/tmp/ivy", \
    "/opt/app/spark_job.py"]