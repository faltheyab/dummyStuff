from pyspark.sql import SparkSession
import threading
import time
from datetime import datetime

# Utility function to print with timestamp
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Thread target function
def sleeper_thread(thread_id):
    for i in range(4):
        log(f"[Thread-{thread_id}] Sleeping for 5 minutes ({i+1}/4)...")
        time.sleep(300)  # 5 minutes
    log(f"[Thread-{thread_id}] Finished sleeping.")

# Main function
def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SimpleDFWithThreads") \
        .getOrCreate()

    # Create and show DataFrame
    data = [("Alice", 30), ("Bob", 25), ("Cathy", 27)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    log("=== DataFrame Created ===")
    df.show()

    # Start daemon threads
    threads = []
    num_threads = 4
    for i in range(num_threads):
        t = threading.Thread(target=sleeper_thread, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    # Optional: small sleep to ensure threads start
    time.sleep(2)

    # Stop Spark
    log("Stopping SparkSession...")
    spark.stop()
    log("SparkSession stopped.")

    # Let daemon threads run for a bit longer before main exits
    time.sleep(60)  # adjust as needed (1 min here)

    log("Main thread exiting. Daemon threads will be terminated.")

if __name__ == "__main__":
    main()
