import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Pour éviter les problèmes de libs natives Hadoop sous Windows
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"

# Version stable : PySpark 3.5.1
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

spark = (
    SparkSession.builder
    .appName("StreamingTransactions")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.hadoop.io.nativeio.use.native", "false")  # <= important
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("merchant_id", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
])

# 1) Lecture du flux Kafka
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .load()
)

# 2) Parsing JSON
df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp("timestamp"))
)

# 3) Règles d’anomalies simples

# Montant > 5000
df = df.withColumn(
    "is_high_amount",
    when(col("amount") > 5000, 1).otherwise(0)
)

# Pays risqués
risky_countries = ["RU", "IR", "KP", "AF"]
df = df.withColumn(
    "is_risky_country",
    when(col("country").isin(risky_countries), 1).otherwise(0)
)

# Score
df = df.withColumn(
    "anomaly_score",
    col("is_high_amount") + col("is_risky_country")
)

df = df.withColumn(
    "is_anomaly",
    when(col("anomaly_score") >= 1, 1).otherwise(0)
)

# 4) On garde seulement les anomalies
anomalies_df = df.filter(col("is_anomaly") == 1)

# 5) Écriture dans le Data Lake
query = (
    anomalies_df
    .writeStream
    .format("parquet")
    .option("path", "../datalake/curated/anomalies")
    .option("checkpointLocation", "../datalake/checkpoints/anomalies")
    .outputMode("append")
    .start()
)

query = (
    anomalies_df
    .writeStream
    .foreachBatch(lambda df, _: df.write.mode("append").parquet("../datalake/curated/anomalies") if df.count() > 0 else None)
    .option("checkpointLocation", "../datalake/checkpoints/anomalies")
    .start()
)


print("✅ Stream Spark démarré, en attente des messages Kafka...")

query.awaitTermination()
