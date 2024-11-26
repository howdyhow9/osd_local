from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
from delta.tables import *

# Function to create Spark session with Delta Lake and Hive metastore support
def create_spark_session():
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "s3a://delta/") \
        .config("hive.metastore.warehouse.dir", "s3a://delta/") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "hive") \
        .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
        .config("datanucleus.schema.autoCreateTables", "true") \
        .config("hive.metastore.schema.verification", "false") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master("local[2]") \
        .enableHiveSupport()

    return configure_spark_with_delta_pip(builder).getOrCreate()

# Create Spark session
spark = create_spark_session()

# Show databases
spark.sql("show databases").show()

# Drop the 'restaurant' database if it exists
spark.sql("drop database if exists restaurant cascade")

spark.sql("show databases").show()

# Function to ingest CSV files into Delta tables and manually create a checkpoint
def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    # Read CSV file into DataFrame
    df = spark.read.option("header", True).option("inferSchema", True).csv(iFilePath)
    df.show()

    # Create schema if it doesn't exist
    spark.sql(f"create schema if not exists {iDBSchema}")

    df.write.option("mergeSchema", True).format("delta").mode("overwrite").saveAsTable(f"{iDBSchema}.{iTable}")

# Ingest CSV files into Delta tables with paths in S3
IngestDeltaCSVHeader("restaurant", "menu", "s3a://source-files/menu_items.csv")
IngestDeltaCSVHeader("restaurant", "orders", "s3a://source-files/order_details.csv")
IngestDeltaCSVHeader("restaurant", "db_dictionary", "s3a://source-files/restaurant_db_data_dictionary.csv")