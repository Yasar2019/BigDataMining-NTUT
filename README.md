# PySpark Power Consumption Analysis

### By: Yasar Nazzarian, 112012049, IEECS

This project analyzes household power consumption data using PySpark. The main aim is to calculate various statistics like minimum, maximum, mean, and standard deviation for different power metrics.


## Environment setup
* CPU : Intel(R) Core(TM) i7-8750H
* Cores : 6
* RAM : 16 GB
## Prerequisites

1. **Python:** This project requires Python 3.x. Download and install it from [Python's official website](https://www.python.org/downloads/).
2. **Spark:** Install Apache Spark. You can follow the installation guide on the [official Spark page](https://spark.apache.org/downloads.html).
3. **PySpark:** Once Spark is installed, you'll need to install the PySpark package:
`pip install pyspark`

4. **Java:** Spark requires Java to run. Ensure that you have Java 8 installed. You can download it from the [official Oracle website](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html).

5. **Scala:** Install Scala as it's a prerequisite for Spark. You can download it from the [official Scala website](https://www.scala-lang.org/download/).

6. **Hadoop and Windows Utilities:** If you are running on Windows, you might need to download winutils.exe and set the `HADOOP_HOME` environment variable. This is necessary to run Spark locally. You can find more about this in various online Spark-on-Windows guides.

## Dataset

Make sure you have the `household_power_consumption.txt` dataset placed in the root directory of the project or adjust the path in the script accordingly.

## Running the Script

1. Navigate to the project directory in your terminal or command prompt.
2. Run the script using the following command:
`spark-submit hm00-112012049.py`

## Output
I provided screenshot of the required stats.
For the normalized data, please check the output file in normalized_data
