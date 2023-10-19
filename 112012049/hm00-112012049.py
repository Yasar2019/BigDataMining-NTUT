from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

import os
print(os.environ['HADOOP_HOME'])


def process_line(line):
    # Splitting the line by delimiter and extracting required fields
    parts = line.split(";")
    try:
        # Check for missing values
        if '?' in parts or '""' in parts:
            return (None, None, None, None)

        # Extracting the columns of interest and converting them to float
        global_active_power = float(parts[2].replace('"', ''))
        global_reactive_power = float(parts[3].replace('"', ''))
        voltage = float(parts[4].replace('"', ''))
        global_intensity = float(parts[5].replace('"', ''))
        return (global_active_power, global_reactive_power, voltage, global_intensity)
    except ValueError as e:
        print(f"Error: {e}")
        return (None, None, None, None)


def stats_map_function(record):
    return ("stats", (
        # For global_active_power
        (record[0], record[0], record[0], record[0]**2),
        # For global_reactive_power
        (record[1], record[1], record[1], record[1]**2),
        (record[2], record[2], record[2], record[2]**2),  # For voltage
        # For global_intensity
        (record[3], record[3], record[3], record[3]**2),
        1  # Count
    ))


def stats_reduce_function(x, y):
    """Reduce function to calculate stats for each column."""
    return (
        (min(x[0][0], y[0][0]), max(x[0][1], y[0][1]),
         x[0][2] + y[0][2], x[0][3] + y[0][3]),
        (min(x[1][0], y[1][0]), max(x[1][1], y[1][1]),
         x[1][2] + y[1][2], x[1][3] + y[1][3]),
        (min(x[2][0], y[2][0]), max(x[2][1], y[2][1]),
         x[2][2] + y[2][2], x[2][3] + y[2][3]),
        (min(x[3][0], y[3][0]), max(x[3][1], y[3][1]),
         x[3][2] + y[3][2], x[3][3] + y[3][3]),
        x[4] + y[4]
    )


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PowerConsumptionStats") \
        .master("local") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config("spark.hadoop.home.dir", "C:/Users/Asus/Downloads/spark-3.5.0-bin-hadoop3/spark-3.5.0-bin-hadoop3/bin") \
        .getOrCreate()

    sc = spark.sparkContext

    # Load the dataset into an RDD
    data_rdd = sc.textFile(
        "household_power_consumption.txt").filter(lambda x: "Global_active_power" not in x)
    processed_rdd = data_rdd.map(process_line).filter(lambda record: None not in record)
    
    # For the stats
    stats_results = processed_rdd.map(stats_map_function).reduceByKey(
        stats_reduce_function).collect()[0][1]

    count = stats_results[4]
    print("Task 1: Minimum, Maximum, and Count")
    print("Global Active Power - Min:",
          stats_results[0][0], ", Max:", stats_results[0][1], ", Count:", count)
    print("Global Reactive Power - Min:",
          stats_results[1][0], ", Max:", stats_results[1][1], ", Count:", count)
    print("Voltage - Min:",
          stats_results[2][0], ", Max:", stats_results[2][1], ", Count:", count)
    print("Global Intensity - Min:",
          stats_results[3][0], ", Max:", stats_results[3][1], ", Count:", count)

    print("\nTask 2: Mean and Standard Deviation")
    for i, name in enumerate(["Global Active Power", "Global Reactive Power", "Voltage", "Global Intensity"]):
        mean = stats_results[i][2] / count
        variance = (stats_results[i][3] / count) - (mean**2)
        stddev = variance**0.5
        print(name, "- Mean:", mean, ", Standard Deviation:", stddev)


    # Task 3: Min-Max Normalization
    def normalize(record):
        return (
            (record[0] - stats_results[0][0]) /
            (stats_results[0][1] - stats_results[0][0]),
            (record[1] - stats_results[1][0]) /
            (stats_results[1][1] - stats_results[1][0]),
            (record[2] - stats_results[2][0]) /
            (stats_results[2][1] - stats_results[2][0]),
            (record[3] - stats_results[3][0]) /
            (stats_results[3][1] - stats_results[3][0])
        )


    normalized_rdd = processed_rdd.map(normalize)
    
    df = normalized_rdd.map(lambda record: Row(
    column1=record[0],
    column2=record[1],
    column3=record[2],
    column4=record[3]
    )).toDF()
    
    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Define custom headers
    headers = ["normalized global active power",
               "normalized global reactive power", "normalized voltage", "normalized global intensity"]
    
    # Use Pandas to write the data to disk
    output_path = "C:/Users/Asus/Desktop/112012049/normalized_data/normalized.csv"
    pandas_df.to_csv(output_path, header=headers, index=False)
    
    # Stop the context
    sc.stop()