from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *


builder = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir","/opt/spark/work-dir/data/delta/osdp/spark-warehouse") \
    .config("hive.metastore.warehouse.dir","/opt/spark/work-dir/data/delta/osdp/spark-warehouse") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
    .config("javax.jdo.option.ConnectionUserName", "hive") \
    .config("javax.jdo.option.ConnectionPassword", "GUYgsjsj@123") \
    .config("datanucleus.schema.autoCreateTables", "true") \
    .config("hive.metastore.schema.verification", "false") \
    .master("local[2]") \
    .enableHiveSupport()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sql("show databases").show()

def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    input_csv = spark.read.option("header", True).csv(iFilePath)
    input_csv.show()
    spark.sql("create schema if not exists "+iDBSchema)
    input_csv.write.format("delta").mode("append").saveAsTable(iDBSchema+"."+iTable)

IngestDeltaCSVHeader("restaurant","k8smenu", "/opt/spark/work-dir/data/source_data/menu_items.csv")


