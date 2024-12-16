# spark_sql_interactive.py
from spark_config_delta import create_spark_session

spark = create_spark_session()
print("Spark SQL session started!")
print("\nAvailable databases:")
spark.sql("SHOW DATABASES").show()

while True:
    query = input("\nEnter SQL query (or 'exit' to quit): ")
    if query.lower() == 'exit':
        break
    try:
        spark.sql(query).show(truncate=False)
    except Exception as e:
        print(f"Error executing query: {str(e)}")

spark.stop()