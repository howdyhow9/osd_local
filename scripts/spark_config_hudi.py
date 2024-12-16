# spark_config.py
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session():
    builder = SparkSession.builder \
        .appName("Spark Hudi with MinIO") \
        .config("spark.jars.packages", 'org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.1') \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.sql.warehouse.dir", "s3a://delta-lake/warehouse/") \
        .config("hive.metastore.warehouse.dir", "s3a://delta-lake/warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/hive_metastore") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "hive") \
        .config("javax.jdo.option.ConnectionPassword", "hivepass123") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("datanucleus.schema.autoCreateTables", "true") \
        .config("hive.metastore.schema.verification", "false") \
        .master("local[2]") \
        .enableHiveSupport()

    return configure_spark_with_delta_pip(builder).getOrCreate()