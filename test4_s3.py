from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import os, calendar, sys, time,random

'''
add these jars: hadoop-aws-3.3.1.jar,aws-java-sdk-bundle-1.12.367.jar
'''

finish_time=(datetime.now() + timedelta(hours=1, minutes=20))

def get_random_int():
    return random.randint(0, 10)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)

log("************----------------------Application started----------------------------************")
spark = SparkSession.builder \
    .appName("Dency pyspark job") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.access.key","bf49816e1eec4368a38b5dc9c57de9a2") \
    .config("spark.hadoop.fs.s3a.secret.key","4f86cd88400b6b1671abc5b1c757a6330eb06a76012ad19b") \
    .config("spark.hadoop.fs.s3a.path.style.access","true") \
    .config("spark.hadoop.fs.s3a.endpoint","s3.jp-tok.cloud-object-storage.appdomain.cloud") \
    .enableHiveSupport() \
    .getOrCreate()
spark.conf.set('spark.sql.caseSensitive', True)
spark.conf.set('spark.sql.files.ignoreMissingFiles', True)
spark.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
sc = spark.sparkContext

log("************----------------------Done S3 configuration----------------------------************")
query = "SELECT ID, DATA FROM aetemptable LIMIT 100000"


def read_write_app(input_name, output_name):
    log("\nSTEP1\n") 
    read_df = spark.read.option("header",True).csv("s3a://faisalsbkt/multiple_csvs/"+input_name)

    log("\nSTEP2\n")
    read_df.createOrReplaceTempView("aetemptable")

    log("\nSTEP3\n")
    query_df = spark.sql(query)

    log("\nSTEP4\n")
    query_df.coalesce(1).write.option("header", True).option("compression", "gzip").mode("overwrite").csv("s3a://faisalsbkt/output/"+output_name)


while True:
    random_num = get_random_int()
    log("#####------------------------Inside loop: random number is "+str(random_num)+"--------------------------######")

    if (random_num % 2 == 0):
        read_write_app("ID_DATA_Example.csv","output_odd_cleanedUp_s3.csv")
    else:
        read_write_app("Employees.csv","output_odd_cleanedUp_s3.csv")

    if (datetime.now() > finish_time):
        break


log("************----------------------Application finished----------------------------************")
log("Stopping...")
sc.stop()

