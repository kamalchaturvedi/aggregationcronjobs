import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, functions as F
from pyspark.sql.types import StringType, LongType
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from time import time_ns
from datetime import datetime, timedelta
from math import pow
import uuid


uuidUdf= F.udf(lambda : str(uuid.uuid4()),StringType())
timestampUdf= F.udf(lambda : time_ns(),LongType())

def run_job(jobtype, cassandra_ip_list):
    
    # export PYSPARK_DRIVER_PYTHON="/usr/local/bin/python3.7"
    # export PYSPARK_PYTHON="/usr/local/bin/python3.7"
    cluster = Cluster(cassandra_ip_list)
    cassandra_session = cluster.connect('iot')
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS {}telemetry (
                id text,
                deviceid text,
                meanval float,
                maxval float,
                minval float,
                stddevval float,
                countval bigint,
                createdTimestamp bigint,
                PRIMARY KEY (id)
            )
    """.format(job_type))
    current_timestamp = int(datetime.timestamp(datetime.now())*pow(10,9))
    if job_type == "minutely":
        last_job_run_time = datetime.now() - timedelta(minutes = 1)
        last_job_run_timestamp = int(datetime.timestamp(last_job_run_time)*pow(10,9))
        job_run_data = cassandra_session.execute('select * from telemetry where createdTimestamp >= {} AND createdTimestamp < {} allow filtering'.format(last_job_run_timestamp, current_timestamp))
    elif job_type == "hourly":
        last_job_run_time = datetime.now() - timedelta(hours = 1)
        last_job_run_timestamp = int(datetime.timestamp(last_job_run_time)*pow(10,9))
        job_run_data = cassandra_session.execute('select * from minutelytelemetry where createdTimestamp >= {} AND createdTimestamp < {} allow filtering'.format(last_job_run_timestamp, current_timestamp))
    elif job_type == "daily":
        last_job_run_time = datetime.now() - timedelta(days = 1)
        last_job_run_timestamp = int(datetime.timestamp(last_job_run_time)*pow(10,9))
        job_run_data = cassandra_session.execute('select * from hourlytelemetry where createdTimestamp >= {} AND createdTimestamp < {} allow filtering'.format(last_job_run_timestamp, current_timestamp))
    sconf=SparkConf().setAppName("cron").setMaster("local[*]")
    sc = SparkContext(conf=sconf)
    sql_context = SQLContext(sc)
    if job_run_data:
        job_run_data = list(job_run_data)
        print("Retrieved {} elements".format(len(job_run_data)))
        df = sql_context.createDataFrame(job_run_data)
        if job_type == "minutely":
            df = df.groupBy("deviceid").agg(F.mean('data').alias('meanval'), F.max('data').alias('maxval'), F.min('data').alias('minval'), F.stddev('data').alias('stddevval'), F.count('id').alias('countval'))
        else:
            df = df.groupBy("deviceid").agg(F.mean('meanval').alias('meanval'), F.max('maxval').alias('maxval'), F.min('minval').alias('minval'), F.mean('stddevval').alias('stddevval'), F.sum('countval').alias('countval'))
        # df = df.groupBy("deviceid").agg({'data':'avg', 'data':'max','data':'min', 'data':'stddev', 'id':'count'})
        df = df.withColumn('createdTimestamp', timestampUdf())
        df = df.withColumn('id', uuidUdf())
        df.printSchema()
        aggregate_result = df.collect()
        # now insert all data to cassandra
        insertion_info = cassandra_session.prepare('insert into {}telemetry (id, deviceid, meanval, maxval, minval, stddevval, countval, createdTimestamp) values (?,?,?,?,?,?,?,?)'.format(job_type))
        for info in aggregate_result:
            batch.add(insertion_info, (info.id, info.deviceid, float(info.meanval), float(info.maxval), float(info.minval), float(info.stddevval), int(info.countval), info.createdTimestamp))
        cassandra_session.execute(batch)
    else:
        print('No elements retrieved from this job run')
    sc.stop()
    cluster.shutdown()

if __name__=="__main__":
    try:
        job_type = sys.argv[1]
        print('Running job : {}'.format(job_type))
        cassandra_ip_list = sys.argv[2];
        cassandra_ip_list = cassandra_ip_list[1:-1]
        cassandra_ip_list = cassandra_ip_list.split(',')
        run_job(job_type, cassandra_ip_list)
    except Exception as e:
        print('Exception : {}'.format(e))