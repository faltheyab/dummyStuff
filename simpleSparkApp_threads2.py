import threading, time
from datetime import datetime
from pyspark.sql import SparkSession

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def task(minutes, spark):
    log("[thread] Working Started - "+str(minutes))
        # Create and show DataFrame
    data = [("Alice", 30), ("Bob", 25), ("Cathy", 27)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    log("=== DataFrame Created ===")
    df.show()
    time.sleep(minutes*60)
    log("[thread] Finished")

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SimpleDFWithThreads") \
        .getOrCreate()
    
    log("[Main] Main thread exiting")
    thread_one = threading.Thread(target=task, args=(15,spark,))
    thread_two = threading.Thread(target=task, args=(20,spark,))
    thread_one.start()
    thread_two.start()
    time.sleep(30)
    # Stop Spark
    log("Stopping SparkSession...")
    spark.stop()
    log("SparkSession stopped.")
    log("[Main] Main thread exiting")


if __name__ == "__main__":
    main()
