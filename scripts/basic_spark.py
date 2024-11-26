from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
#from spark_config_hive_ms import create_spark_session

# spark_config.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session():
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir","s3a://delta/") \
        .config("hive.metastore.warehouse.dir","s3a://delta/") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres.spark-apps:5432/hive_metastore") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "hive") \
        .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
        .config("datanucleus.schema.autoCreateTables", "true") \
        .config("hive.metastore.schema.verification", "false") \
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master("local[2]") \
        .enableHiveSupport()

    return configure_spark_with_delta_pip(builder).getOrCreate()

spark = create_spark_session()
spark.sql("show databases").show()

def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    menu_csv = spark.read.option("header", True).option("inferSchema", True).csv(iFilePath)
    menu_csv.show()
    
    spark.sql("create schema if not exists "+iDBSchema)
    menu_csv.write.option("mergeSchema", True).format("delta").mode("overwrite").saveAsTable(iDBSchema+"."+iTable)

IngestDeltaCSVHeader("restaurant","menu", "/opt/spark/work-dir/data/source_data/menu_items.csv")
IngestDeltaCSVHeader("restaurant","orders", "/opt/spark/work-dir/data/source_data/order_details.csv")
IngestDeltaCSVHeader("restaurant","db_dictionary", "/opt/spark/work-dir/data/source_data/restaurant_db_data_dictionary.csv")

