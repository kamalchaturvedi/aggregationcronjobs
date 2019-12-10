FROM ubuntu:19.10
RUN apt-get update
RUN apt-get install -y python3 python3-pip git
RUN python3 -m pip install pyspark cassandra-driver
COPY aggregation_jobs.py .
ARG cassandra_nodes_list
ENV cassandra_nodes_list ${cassandra_nodes_list}
CMD python3 aggregation_jobs.py ${job_type} ${cassandra_nodes_list}