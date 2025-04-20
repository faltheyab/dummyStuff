from pyspark.sql import SparkSession
import threading
import time
import random
from datetime import datetime

def print_current_datetime():
    now = datetime.now()
    readable_time = now.strftime("%A, %B %d, %Y at %I:%M:%S %p")
    return readable_time

# Function that will run in daemon threads
def background_worker(thread_id):
    while True:
        print(print_current_datetime()+f" [Thread-{thread_id}] Doing background work...")
        time.sleep(random.uniform(1, 3))  # Simulate variable work time

# Main Spark application
def main():
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DaemonThreadsInSpark") \
        .master("local[*]") \
        .getOrCreate()

    print( print_current_datetime() + "🚀 SparkSession started")

    # Create and start daemon threads
    for i in range(4):
        t = threading.Thread(target=background_worker, args=(i,))
        t.daemon = True  # Make thread a daemon
        t.start()
        print(print_current_datetime() + f"🧵 Daemon thread {i} started")

    # Spark part – dummy transformation
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)
    squared = rdd.map(lambda x: x * x).collect()

    print(print_current_datetime() + f"✅ Result of Spark transformation: {squared}")

    # Keep the main thread alive for a while to let daemon threads run
    print(print_current_datetime() + "🕒 Waiting to observe daemon threads...")
    minutes = 5
    # Convert minutes to seconds
    print(print_current_datetime() + f"Waiting for {minutes} minutes...")
    time.sleep(minutes * 60)

    # Done
    print(print_current_datetime() + "🛑 Done. Spark will stop, daemon threads will also terminate.")

    spark.stop()

if __name__ == "__main__":
    main()