FROM ubuntu:19.10
RUN apt-get update
RUN apt-get install -y python3 python3-pip git
RUN python3 -m pip install pyspark cassandra-driver
COPY aggregation_jobs.py .
ARG cassandra_node
ENV cassandra_node ${cassandra_node}
ENV PYSPARK_DRIVER_PYTHON /usr/local/bin/python3.7
ENV PYSPARK_PYTHON /usr/local/bin/python3.7
CMD python3 aggregation_jobs.py ${job_type} ${cassandra_node}