from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import os, calendar, sys, time,random


def get_random_int():
    return random.randint(0, 10)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

log("--------------------------------------------------")
log("--------------------------------------------------")
log("Application started")
# INIT
spark = SparkSession.builder.appName("service").enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.caseSensitive', True)
spark.conf.set('spark.sql.files.ignoreMissingFiles', True)
spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
#------
spark.conf.set("spark.hadoop.fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
spark.conf.set("spark.hadoop.fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.S3AFileSystem")

spark.conf.set("spark.hadoop.fs.stocator.connection.timeout.ms", "60000")       # 60 seconds
spark.conf.set("spark.hadoop.fs.stocator.read.timeout.ms", "120000")            # Optional, 2 minutes read timeout
spark.conf.set("spark.hadoop.fs.stocator.retry.count", "5")                     # Retry limit
spark.conf.set("spark.hadoop.fs.stocator.retry.interval.ms", "5000")            # Retry interval
#spark.conf.set("spark.executor.heartbeatInterval", "30s") results an error - application is not running
#spark.conf.set("spark.network.timeout", "300s") results an error - application is not running
spark.conf.set("spark.hadoop.fs.stocator.write.timeout.ms", "60000")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#-----
sc = spark.sparkContext
hconf=sc._jsc.hadoopConfiguration()
hconf.set("fs.cos.cloudobjectstorage.endpoint", "s3.jp-tok.cloud-object-storage.appdomain.cloud")
hconf.set("fs.cos.cloudobjectstorage.access.key", "bf49816e1eec4368a38b5dc9c57de9a2") 
hconf.set("fs.cos.cloudobjectstorage.secret.key", "4f86cd88400b6b1671abc5b1c757a6330eb06a76012ad19b")
hconf.set("fs.cos.cloudobjectstorage.iam.api.key","6uQQFo7Fc-bSUE4Vlym7aOeIh_QbwT3AdAz6Oj_dCgmF")
hconf.set("fs.stocator.scheme.list", "cos")
hconf.set("fs.stocator.cos.scheme", "cos")
hconf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")#
hconf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
log("Done COS configuration")
query = "SELECT ID, DATA FROM aetemptable LIMIT 100000"


def check_spark_jobs():
    log("\n\n-------------------Checking jobs-------------\n\n")
    # Check the number of active jobs, stages, or tasks
    active_jobs = sc.statusTracker().getActiveJobsIds()
    if not active_jobs:
        log("No active jobs remaining")
    else:
        log(f"Active jobs: {active_jobs}")
    log("--------------------------------------------------")

def read_write_app(input_name, output_name):
    #log("\nSTEP1\n")
    read_df = spark.read.option("header", True).csv("cos://faisalsbkt.cloudobjectstorage/multiple_csvs/"+input_name)

    #log("\nSTEP2\n")
    read_df.createOrReplaceTempView("aetemptable")

    #log("\nSTEP3\n")
    query_df = spark.sql(query)
    #query_df.show()

    #log("\nSTEP4\n")
    query_df.coalesce(1).write.option("header", True).option("compression", "gzip").mode("overwrite").csv("cos://faisalsbkt.cloudobjectstorage/output/"+output_name)

    spark.catalog.dropTempView("aetemptable")

    # if hasattr(query_df, "isStreaming") and query_df.isStreaming:
    #     log("\nis streaming\n")
    #     query_df.awaitTermination()

finish_time=(datetime.now() + timedelta(hours=1,minutes=20))
while True:
    #log("--------------------------------------------------")
    random_num = get_random_int()
    #log("Inside loop: random number is "+str(random_num))
    
    if (random_num % 2 == 0):
        read_write_app("ID_DATA_Example.csv","output_even_15min.csv")
    else:
        read_write_app("Employees.csv","output_odd_15min.csv")
    
    check_spark_jobs()
    if (datetime.now() > finish_time):
        break
    #log("--------------------------------------------------")
check_spark_jobs()
log("\nFINISHED - Stopping spark context\n")
sc.stop()
log("--------------------Stopping spark object-----------")
spark.stop()
log("-------------------Sleeping for 5 sec---------------")
time.sleep(10)
log("------------------sys exit--------------------------")
sys.exit(0)
log("------------------os exit---------------------------")
os._exit(0)