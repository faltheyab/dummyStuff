from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import os, sys, time,random

finish_time=(datetime.now() + timedelta(hours=1,minutes=5))

def get_random_int():
    return random.randint(0, 10)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

log("************----------------------Application started----------------------------************")

# INIT Config by customer
spark = SparkSession.builder.appName("service").enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.caseSensitive', True)
spark.conf.set('spark.sql.files.ignoreMissingFiles', True)
spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
#------
spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "64")
spark.conf.set("spark.hadoop.fs.s3a.threads.max", "64")
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
hconf.set("fs.s3a.connection.maximum", "64")
hconf.set("fs.s3a.threads.max", "64")
log("************----------------------Done COS configuration----------------------------************")
query = "SELECT ID, DATA FROM aetemptable LIMIT 100000"

def read_write_app(input_name, output_name):
    log("\nSTEP1\n")
    read_df = spark.read.option("header", True).csv("cos://faisalsbkt.cloudobjectstorage/multiple_csvs/"+input_name)
    read_df.count() 

    log("\nSTEP2\n")
    read_df.createOrReplaceTempView("aetemptable")
    
    log("\nSTEP3\n")
    query_df = spark.sql(query)
    query_df.show()

    log("\nSTEP4\n")
    query_df.repartition(1).write.option("header", True).option("compression", "gzip").mode("overwrite").csv("cos://faisalsbkt.cloudobjectstorage/output/"+output_name)

    spark.catalog.dropTempView("aetemptable")
    query_df.unpersist()
    read_df.unpersist()


while True:
    random_num = get_random_int()
    log("#####------------------------Inside loop: random number is "+str(random_num)+"--------------------------######")

    if (random_num % 2 == 0):
        read_write_app("ID_DATA_Example.csv","output_odd_cleanedUp.csv")
    else:
        read_write_app("Employees.csv","output_odd_cleanedUp.csv")
    
    if (datetime.now() > finish_time):
        break


log("************----------------------Application finished----------------------------************")
log("Stopping: spark context -> spark session -> sleep 10 seconds -> sys.exit(0) -> os._exit(0)")
sc.stop()
spark.stop()
time.sleep(10)
sys.exit(0)
os._exit(0)
