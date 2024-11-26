
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import time
import json
from spark_config import create_spark_session
import logging  # Import the logging module

# Configure logging
#logging.basicConfig(level=logging.INFO)

# Create Spark session
spark = create_spark_session()
spark.sparkContext.setLogLevel("INFO")

# Get the underlying log4j logger
#logger = spark._jvm.org.apache.log4j

# Set the log level to INFO
#logger.LogManager.getLogger("org").setLevel(logger.Level.INFO)

# db_schema = 'public'
# table = 'actor'

def run_replication(i_dbschema, i_table):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", "cresql_db.cresql."+i_dbschema+"."+i_table) \
        .option("startingOffsets", "earliest") \
        .load()

    logging.info("Reading data from Kafka topic")

    chk_pt = "/opt/spark/work-dir/data/delta/osdp/spark-warehouse/chk_pt/"+i_dbschema+"/"+i_table+"/"
    table_loc = "/opt/spark/work-dir/data/delta/osdp/spark-warehouse/"+i_dbschema+".db/"+i_table+"/"

    def process_micro_batch_merge(i_df, epoch_id):

        logging.info("Starting micro batch processing")

        df_key = i_df.selectExpr("CAST(key AS STRING)").limit(1)
        df_key_json = spark.read.json(df_key.rdd.map(lambda r: r.key))
        pks = df_key_json.columns
        pks_str = ','.join(pks)

        join_stmt = ""
        pks_lst = []
        for pk in pks:
            x = "updates."+pk+" = "+"target."+pk
            pks_lst.append(x)
            join_stmt = ','.join(pks_lst)
            
        print(pks)
        print(pks_str)
        print(pks_lst)
        print(join_stmt) 
        

        logging.info(f"Join statement: {join_stmt}")

        logging.info("Processing JSON data")


        df_value = i_df.selectExpr("CAST(value AS STRING)")
        df_json = spark.read.json(df_value.rdd.map(lambda r: r.value))
        df_json.show(truncate=False)

        logging.info("Deduplicating data")

        #dedup
        window_spec = Window.partitionBy(*pks).orderBy(col("___meta_event_ts").desc())

        # Add a rank column based on the lsn column within each partition
        df_with_rank = df_json.withColumn("rank", F.row_number().over(window_spec))

        # Deduplicate based on rank and primary key (keeping only the latest record for each primary key)
        dedup_df = df_with_rank.filter(col("rank") == 1).drop("rank")


        logging.info("Writing data to Delta table")


        if not DeltaTable.isDeltaTable(spark, table_loc):
            logging.info("Delta table does not exist, creating new one")
            dedup_df.write.format("delta").mode("append").save(table_loc)
            spark.sql("create schema if not exists "+i_dbschema+"")
            spark.sql("create table if not exists "+i_dbschema+"."+i_table+" using delta location '"+table_loc+"'")

        else:
            logging.info("Merging data with existing Delta table")
            target_df = DeltaTable.forPath(spark, table_loc)

            target_df.alias('target') \
                .merge(
                dedup_df.alias('updates'),
                join_stmt
            ) \
                .whenNotMatchedInsertAll(condition = "updates.___meta_op = 'c' or updates.___meta_op = 'r'") \
                .whenMatchedUpdateAll(condition = "updates.___meta_op = 'u'") \
                .whenMatchedDelete(condition = "updates.___meta_op = 'd'").execute()
            #remove above line if delete needs to be persisted
            logging.info("Micro batch processing completed")

    merging_sink = df.writeStream.option("checkpointLocation", chk_pt).trigger(processingTime='15 seconds').foreachBatch(process_micro_batch_merge).start()
    merging_sink.awaitTermination()

#run_replication('Sales', 'SalesOrderDetail')
run_replication('dbo', 'Inventory')