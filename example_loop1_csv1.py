from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import os, calendar, sys, time

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

# INIT
spark = SparkSession.builder.appName("service").enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.caseSensitive', True)
spark.conf.set('spark.sql.files.ignoreMissingFiles', True)
spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
sc = spark.sparkContext
hconf=sc._jsc.hadoopConfiguration()
hconf.set("fs.cos.cloudobjectstorage.endpoint", "s3.jp-tok.cloud-object-storage.appdomain.cloud")
hconf.set("fs.cos.cloudobjectstorage.access.key", "bf49816e1eec4368a38b5dc9c57de9a2") 
hconf.set("fs.cos.cloudobjectstorage.secret.key", "4f86cd88400b6b1671abc5b1c757a6330eb06a76012ad19b")
hconf.set("fs.cos.cloudobjectstorage.iam.api.id","ApiKey-da8f0866-23e3-4f65-b19c-c37e1af20449")
hconf.set("fs.cos.cloudobjectstorage.iam.api.key","6uQQFo7Fc-bSUE4Vlym7aOeIh_QbwT3AdAz6Oj_dCgmF")
hconf.set("fs.cos.cloudobjectstorage.iam.role.crn","crn:v1:bluemix:public:iam::::serviceRole:Manager")
hconf.set("fs.cos.cloudobjectstorage.iam.api.key","6uQQFo7Fc-bSUE4Vlym7aOeIh_QbwT3AdAz6Oj_dCgmF")
hconf.set("fs.cos.cloudobjectstorage.iam.serviceid.crn","crn:v1:bluemix:public:iam-identity::a/1836f77885e521c5ab2523aac93b485e::serviceid:ServiceId-aecf830c-33fa-4d07-a72e-41acc5434f19")
hconf.set("fs.cos.cloudobjectstorage.resource.instance.id","crn:v1:bluemix:public:cloud-object-storage:global:a/1836f77885e521c5ab2523aac93b485e:3bffe2ac-53e9-4af4-adeb-eb37655b85f1::")
hconf.set("fs.stocator.scheme.list", "cos")
hconf.set("fs.stocator.cos.scheme", "cos")
hconf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")#
hconf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")

query = "SELECT ID, DATA FROM aetemptable LIMIT 100000"

loop=1
for i in range(loop):
    log("\nCYCLE " + str(i+1) + " of " + str(loop) + "\n")
    
    log("\nSTEP1\n")
    read_df = spark.read.option("header", True).csv("cos://faisalsbkt.cloudobjectstorage/one_csv/*")

    log("\nSTEP2\n")
    read_df.createOrReplaceTempView("aetemptable")

    log("\nSTEP3\n")
    query_df = spark.sql(query)
    query_df.show()
    
    log("\nSTEP4\n")
    query_df.coalesce(1).write.option("header", True).option("compression", "gzip").mode("overwrite").csv("cos://faisalsbkt.cloudobjectstorage/output/output2.csv")

log("\nFINISHED\n")

spark.stop()
sys.exit(0)
