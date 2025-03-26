import time
import os
from pyspark.sql import SparkSession

def wait_for(minutes):
    seconds = minutes * 60  # Convert minutes to seconds
    print(f"Waiting for {minutes} minutes...")
    time.sleep(seconds)
    print("Done waiting!")


def startSparkApp():
    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("ExampleSparkApplication") \
        .getOrCreate()

    # Create a new DataFrame manually (example with columns: name, age, gender)
    data = [("Alice", 30, "Female"),
            ("Bob", 25, "Male"),
            ("Catherine", 35, "Female"),
            ("David", 40, "Male"),
            ("Eve", 22, "Female")]

    columns = ["name", "age", "gender"]

    # Create a DataFrame from the list of tuples
    df = spark.createDataFrame(data, columns)

    # Show the first few rows of the DataFrame
    df.show()

    # Perform some basic transformations
    # For example: Filter rows where age > 30
    filtered_df = df.filter(df.age > 30)

    # Show the filtered result
    filtered_df.show()

    # Perform a groupBy operation and get the average age by gender
    avg_age_df = df.groupBy("gender").avg("age")

    # Show the aggregated result
    avg_age_df.show()

    # wait for some time
    print("1 minute")
    for i in range(1):
        wait_for(1)
    # Stop the Spark session
    spark.stop()

def main():
    print("Starting APP")
    startSparkApp()

if __name__ == "__main__":
    main()