# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, window, avg
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import os
from pyspark.sql.streaming import DataStreamWriter

# COMMAND ----------

# DBTITLE 1,Setting Variables - Kafka
kafka_username = os.getenv("kafka_username")
kafka_password = os.getenv("kafka_password")
kafka_server = os.getenv("kafka_server")

# Kafka configurations
kafka_bootstrap_servers = kafka_server
kafka_security_protocol = "SASL_SSL"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";'
kafka_topic = "topic_0"

# COMMAND ----------

# DBTITLE 1,Setting Variables - SQL Server
# SQL Server Configurations
sql_server_name = os.getenv("sql_server_name")
sql_server_port = 1433
sql_server_database = "master"
sql_server_table = "iot_telemetry"
sql_server_username = os.getenv("sql_server_username")
sql_server_password = os.getenv("sql_server_password")
jdbc_url = f"jdbc:sqlserver://{sql_server_name}:{sql_server_port};databaseName={sql_server_database}"

# COMMAND ----------

# DBTITLE 1,Define schema

# Define schema
schema = ArrayType(
    StructType([
        StructField("temperature", StringType(), True)
    ])
)

# COMMAND ----------

# DBTITLE 1,Read from Kafka
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# COMMAND ----------

#df.display()

# COMMAND ----------

# DBTITLE 1,Process the data

# Process the data
val_df = df \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("room", col("key").cast(StringType())) \
    .withColumn("temp", explode(from_json(col("value").cast("string"), schema))) \
    .withColumn("temperature", col("temp.temperature").cast("string")) \
    .select("timestamp", "room", "temperature")

# COMMAND ----------

# DBTITLE 1,Aggregation logic
# Aggregation logic
groupCols = ["room"]
cols = [col(c) for c in groupCols] + [window(col("timestamp"), "20 seconds")]
tumblingWindowDF = val_df \
    .withWatermark("timestamp", "20 seconds") \
    .groupBy(*cols) \
    .agg(avg("temperature").alias("avg_temperature")) \
    .withColumn("timestamp", col("window.start")) \
    .withColumn("temperature", col("avg_temperature")) \
    .select("timestamp", "room", "avg_temperature") \
    .na.drop()

# COMMAND ----------

# DBTITLE 1,Load into SQL Server
#Load data into SQL Server
def foreach_batch_function(batch_df, batch_id):
    # Write the DataFrame to SQL Server
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", sql_server_table) \
        .option("user", sql_server_username) \
        .option("password", sql_server_password) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

query = tumblingWindowDF.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .start()
