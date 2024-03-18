from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "UniqueVisitors")

# Step 1: Load Data
log_rdd = sc.textFile("data.txt")

# Step 2: Extract Date and User Information
date_user_rdd = log_rdd.map(lambda line: (line.split()[0], line.split()[1]))

# Step 3: Map-Reduce
unique_visitors_per_day = date_user_rdd.distinct() \
                                        .map(lambda x: (x[0], 1)) \
                                        .reduceByKey(lambda a, b: a + b)

# Step 4: Action
result = unique_visitors_per_day.collect()

# Print the result
for date, count in result:
    print(f"Date: {date}, Unique Visitors: {count}")

# Stop SparkContext
sc.stop()
