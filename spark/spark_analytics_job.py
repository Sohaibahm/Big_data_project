import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as _sum,
    avg,
    date_trunc
)

# Hadoop pour Windows (comme dans le job streaming)
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"

spark = (
    SparkSession.builder
    .appName("AnalyticsTransactions")
    .config("spark.hadoop.io.nativeio.use.native", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 📂 chemins de ton data lake
BASE_PATH = "../datalake"
CURATED_ANOMALIES = f"{BASE_PATH}/curated/anomalies"
ANALYTICS_BASE = f"{BASE_PATH}/analytics"

print("📥 Lecture des anomalies depuis :", CURATED_ANOMALIES)
anomalies_df = spark.read.parquet(CURATED_ANOMALIES)

# On s'assure que event_time est bien en timestamp
anomalies_df = anomalies_df.withColumn(
    "event_time",
    col("event_time").cast("timestamp")
)

anomalies_df.createOrReplaceTempView("anomalies")

# ========== 1) Anomalies par pays ==========
anomalies_by_country = (
    anomalies_df
    .groupBy("country")
    .agg(
        count("*").alias("nb_anomalies"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .orderBy(col("nb_anomalies").desc())
)

out_country = f"{ANALYTICS_BASE}/anomalies_by_country"
print("💾 Écriture:", out_country)
(
    anomalies_by_country
    .coalesce(1)  # 1 seul fichier CSV pour faciliter l'import Power BI
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(out_country)
)

# ========== 2) Anomalies par canal ==========
anomalies_by_channel = (
    anomalies_df
    .groupBy("channel")
    .agg(
        count("*").alias("nb_anomalies"),
        _sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
    .orderBy(col("nb_anomalies").desc())
)

out_channel = f"{ANALYTICS_BASE}/anomalies_by_channel"
print("💾 Écriture:", out_channel)
(
    anomalies_by_channel
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(out_channel)
)

# ========== 3) Anomalies par minute & pays ==========
anomalies_by_time_country = (
    anomalies_df
    .groupBy(
        date_trunc("minute", col("event_time")).alias("minute"),
        col("country")
    )
    .agg(
        count("*").alias("nb_anomalies"),
        _sum("amount").alias("total_amount")
    )
    .orderBy("minute", "country")
)

out_time_country = f"{ANALYTICS_BASE}/anomalies_by_time_country"
print("💾 Écriture:", out_time_country)
(
    anomalies_by_time_country
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(out_time_country)
)

print("✅ Analytics terminé.")
spark.stop()
