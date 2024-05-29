from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CrudeOilAnalysis") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "/opt/spark/warehouse") \
    .getOrCreate()

# Load Dataset
df = spark.read.csv('/opt/spark/project/data/data.csv', header=True, inferSchema=True)

# Question 1: Top 5 destinations for oil produced in Albania
albania_df = df.filter(df['originName'] == 'Albania') \
    .groupBy('destinationName') \
    .sum('quantity') \
    .withColumnRenamed("sum(quantity)", "quantity") \
    .orderBy('quantity', ascending=False) \
    .limit(5)

# DF -> Iceberg format
spark.sql("""
CREATE TABLE IF NOT EXISTS spark_catalog.default.albania_top5_destinations (
  destinationName STRING,
  quantity BIGINT
) USING iceberg
""")
albania_df.write.format("iceberg").mode("overwrite").save("spark_catalog.default.albania_top5_destinations")

# Verify that the data has been written to the Iceberg table
written_df = spark.read.format("iceberg").load("spark_catalog.default.albania_top5_destinations")

print("\nData written to Iceberg table 'albania_top5_destinations':")
written_df.show(truncate=False)

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
