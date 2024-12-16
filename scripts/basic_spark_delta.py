from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *

from spark_config_delta import create_spark_session

spark = create_spark_session()

spark.sql("show databases").show()

def IngestDeltaCSVHeader(iDBSchema, iTable, iFilePath):
    menu_csv = spark.read.option("header", True).csv(iFilePath)
    menu_csv.show()
    spark.sql("create schema if not exists "+iDBSchema)
    menu_csv.write.format("delta").mode("overwrite").saveAsTable(iDBSchema+"."+iTable)

IngestDeltaCSVHeader("restaurant","menu", "s3a://source-data/menu_items.csv")
IngestDeltaCSVHeader("restaurant","orders", "s3a://source-data/order_details.csv")
IngestDeltaCSVHeader("restaurant","db_dictionary", "s3a://source-data/data_dictionary.csv")

#new_table_df = spark.sql("SELECT * FROM restaurant.orders o join restaurant.menu m on o.item_id = m.menu_item_id")
#new_table_df.write.format("delta").mode("overwrite").saveAsTable("restaurant.newtable")