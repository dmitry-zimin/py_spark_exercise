from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CrudeOilAnalysis") \
    .getOrCreate()

# Load Dataset
df = spark.read.csv('/opt/spark/project/data/data.csv', header=True, inferSchema=True)

# Question 1: Top 5 destinations for oil produced in Albania
albania_df = df.filter(df['originName'] == 'Albania') \
    .groupBy('destinationName') \
    .sum('quantity') \
    .orderBy('sum(quantity)', ascending=False) \
    .limit(5)

# Question 2: For UK, destinations with total quantity > 100,000
uk_destinations_df = df.filter(df['originName'] == 'United Kingdom') \
    .groupBy('destinationName') \
    .sum('quantity') \
    .filter(F.col('sum(quantity)') > 100000)

# Question 3: Most exported grade for each year and origin
exported_grade_df = df.groupBy('year', 'originName', 'gradeName') \
    .sum('quantity') \
    .withColumnRenamed('sum(quantity)', 'TotalQuantity') \
    .orderBy('year', 'originName', 'TotalQuantity', ascending=False)

# Get the most exported grade for each year and origin
window_spec = Window.partitionBy('year', 'originName').orderBy(F.desc('TotalQuantity'))
most_exported_grade_df = exported_grade_df.withColumn('rank', F.rank().over(window_spec)) \
    .filter('rank == 1').drop('rank')

# Show results for verification
print("\nTop 5 destinations for oil produced in Albania:")
albania_df.show(truncate=False)

print("\nDestinations in the UK with total quantity > 100,000:")
uk_destinations_df.show(truncate=False)

print("\nMost exported grade for each year and origin:")
most_exported_grade_df.show(truncate=False)

spark.stop()