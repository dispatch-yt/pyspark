
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Sample Spark Job") \
        .getOrCreate()

    # Create some sample data
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])

    # Print schema and show data
    df.printSchema()
    df.show()

    # Stop the SparkSession
    spark.stop()
