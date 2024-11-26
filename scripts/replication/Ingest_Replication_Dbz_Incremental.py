from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta import *
import time
import json
import boto3
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

s3 = boto3.client('s3')
bucket_name = 'osd-scripts'
s3_file_key = 'spark_config.py'
local_file_path = '/tmp/spark_config.py'
s3.download_file(bucket_name, s3_file_key, local_file_path)
sys.path.append('/tmp')
from spark_config import create_spark_session

# Create Spark session
spark = create_spark_session()
spark.sparkContext.setLogLevel("INFO")

def run_replication(i_dbschema, i_table, i_topic):

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "osds-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092") \
        .option("failOnDataLoss", "false") \
        .option("subscribe", i_topic+"."+i_dbschema+"."+i_table) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Reading data from Kafka topic "+i_topic+"."+i_dbschema+"."+i_table)

    schema_name = "raw_" + i_dbschema

    chk_pt = "s3a://osds-data/raw/chk_pt/"+schema_name+"/"+i_table+"/"
    table_loc = "s3a://osds-data/raw/"+schema_name+".db/"+i_table+"/"
    history_table_loc = "s3a://osds-data/raw/"+schema_name+".db/"+"history_"+i_table+"/"

    def process_micro_batch_merge(i_df, epoch_id):

        print("Starting micro batch processing "+i_topic+i_dbschema+"."+i_table)

        df_key = i_df.selectExpr("CAST(key AS STRING)").limit(1)
        df_key_json = spark.read.json(df_key.rdd.map(lambda r: r.key))
        pks = df_key_json.columns
        pks_str = ','.join(pks)

        join_stmt = ""
        pks_lst = []
        for pk in pks:
            x = "updates."+pk+" = "+"target."+pk
            pks_lst.append(x)
            join_stmt = " AND ".join(pks_lst)

        print(f"Join statement: {join_stmt}")

        print("Processing JSON data "+i_topic+"."+i_dbschema+"."+i_table)


        df_value = i_df.selectExpr("CAST(value AS STRING)")
        df_json = spark.read.json(df_value.rdd.map(lambda r: r.value))
        print("Deduplicating data "+i_topic+"."+i_dbschema+"."+i_table)


        # Deduplication logic
        window_spec = Window.partitionBy(*pks).orderBy(col("___meta_event_ts").desc())

        df_with_rank = df_json.withColumn("rank", F.row_number().over(window_spec))
        dedup_df = df_with_rank.filter(col("rank") == 1).drop("rank")

        print("Writing data to Delta table"+" "+i_dbschema+"."+i_table)

        if not DeltaTable.isDeltaTable(spark, table_loc):
            print("Delta table does not exist, creating new one"+" "+schema_name+"."+i_table)
            dedup_df.write.format("delta").mode("append").save(table_loc)
            spark.sql("create schema if not exists "+schema_name+"")
            spark.sql("create table if not exists "+schema_name+"."+i_table+" using delta location '"+table_loc+"'")
        else:
            spark.sql("create schema if not exists "+schema_name+"")
            spark.sql("create table if not exists "+schema_name+"."+i_table+" using delta location '"+table_loc+"'")
            print("Merging data with existing Delta table"+" "+schema_name+"."+i_table)
            target_df = DeltaTable.forPath(spark, table_loc)

            target_df.alias('target') \
                .merge(
                dedup_df.alias('updates'),
                join_stmt
            ) \
                .whenNotMatchedInsertAll(condition="updates.___meta_op = 'c' or updates.___meta_op = 'r'") \
                .whenMatchedUpdateAll(condition="updates.___meta_op = 'u'") \
                .whenMatchedDelete(condition="updates.___meta_op = 'd'").execute()

            print("Micro batch processing completed"+" "+schema_name+"."+i_table)

        if not DeltaTable.isDeltaTable(spark, history_table_loc):
            spark.sql("create schema if not exists "+schema_name+"")
            df_json.write.format("delta").mode("overwrite").save(history_table_loc)
            spark.sql("create table if not exists "+schema_name+"."+"history_"+i_table+" using delta location '"+history_table_loc+"'")
        else:
            spark.sql("create schema if not exists "+schema_name+"")
            df_json.write.format("delta").mode("append").save(history_table_loc)
            spark.sql("create table if not exists "+schema_name+"."+"history_"+i_table+" using delta location '"+history_table_loc+"'")

    merging_sink = df.writeStream.option("checkpointLocation", chk_pt).trigger(processingTime='5 minutes').foreachBatch(process_micro_batch_merge).start()
    merging_sink.awaitTermination()

# List of schemas and tables to process
replication_tasks = [
    ('dbo', 'Inventory', 'store_db.storedb'),
    ('dbo', 'Invoice_Totals', 'store_db.storedb'),
    ('dbo', 'Invoice_Itemized', 'store_db.storedb'),
    ('dbo', 'Invoice_OnHold', 'store_db.storedb'),
    ('dbo', 'Inventory_In', 'store_db.storedb')
]

# Function to run replication tasks with delay
def submit_task_with_delay(executor, delay, dbschema, table, topic):
    time.sleep(delay)
    future = executor.submit(run_replication, dbschema, table, topic)
    return future

# Use ThreadPoolExecutor to run replication tasks with a 3-minute delay between each one
delay_interval = 180  # 3 minutes in seconds
with ThreadPoolExecutor(max_workers=len(replication_tasks)) as executor:
    futures = {}

    for index, (dbschema, table, topic) in enumerate(replication_tasks):
        delay = delay_interval * index
        future = submit_task_with_delay(executor, delay, dbschema, table, topic)
        futures[future] = (dbschema, table)
        print(f"Submitted replication task for {dbschema}.{table} with {delay//60} minute(s) delay")

    # Wait for all futures to complete
    for future in futures:
        dbschema, table = futures[future]
        try:
            future.result()
            print(f"Replication completed for {dbschema}.{table}")
        except Exception as e:
            print(f"Error during replication for {dbschema}.{table}: {e}")
