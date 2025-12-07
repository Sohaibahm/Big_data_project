"""
streaming_app.py
----------------

Pipeline Spark Structured Streaming :
- Lecture temps réel des transactions depuis Kafka
- Parsing JSON → DataFrame structuré
- Détection d’anomalies (montant trop élevé)
- Écriture en continu dans un data lake (Parquet)
- Affichage des anomalies dans la console

Commande d’exécution (exemple) :
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      streaming_app.py
"""



from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# =====================================================================
# 1. CONFIGURATION GENERALE
# =====================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "transactions"

# chemins pour Windows (tu peux ajuster si tu veux un autre endroit)
BASE_PATH = "file:///C:/Users/nawre/Downloads/projet big data"
OUTPUT_PATH_ALL = BASE_PATH + "/datalake/transactions/all"
OUTPUT_PATH_ANOMALIES = BASE_PATH + "/datalake/transactions/anomalies"
CHECKPOINT_ALL = BASE_PATH + "/checkpoints/transactions_all"
CHECKPOINT_ANOMALIES = BASE_PATH + "/checkpoints/transactions_anomalies"

ANOMALY_THRESHOLD = 3000.0  # seuil montant anomalie


def main():
    # =================================================================
    # 2. Création SparkSession + ajout du package Kafka
    # =================================================================
    spark = SparkSession.builder \
        .appName("StreamingTransactionsAnomalyDetection") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== Spark Streaming - Détection d’anomalies ===\n")

    # =================================================================
    # 3. Définition du schéma JSON des transactions
    # =================================================================
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
    ])

    # =================================================================
    # 4. Lecture du flux Kafka
    # =================================================================
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # la colonne message Kafka (bytes) est convertie en string
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

    # =================================================================
    # 5. Parsing JSON → colonnes structurées
    # =================================================================
    tx_df = json_df \
        .select(from_json(col("json_str"), transaction_schema).alias("data")) \
        .select("data.*")

    # conversion du timestamp texte → Timestamp Spark
    tx_df = tx_df.withColumn("event_time", to_timestamp(col("timestamp")))

    # =================================================================
    # 6. Détection d’anomalies simple
    # =================================================================
    annotated_df = tx_df \
        .withColumn(
            "is_anomaly",
            when(col("amount") > ANOMALY_THRESHOLD, lit(True)).otherwise(lit(False))
        ) \
        .withColumn(
            "anomaly_reason",
            when(col("amount") > ANOMALY_THRESHOLD, lit("HIGH_AMOUNT")).otherwise(lit(None))
        )

    anomalies_df = annotated_df.filter(col("is_anomaly") == True)

    # =================================================================
    # 7. Ecriture dans le Data Lake
    # =================================================================

    # 7.1. Toutes les transactions
    query_all = annotated_df.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH_ALL) \
        .option("checkpointLocation", CHECKPOINT_ALL) \
        .outputMode("append") \
        .start()

    # 7.2. Uniquement les anomalies
    query_anomalies = anomalies_df.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH_ANOMALIES) \
        .option("checkpointLocation", CHECKPOINT_ANOMALIES) \
        .outputMode("append") \
        .start()

    # 7.3. Affichage console (debug)
    console_query = anomalies_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()

    print("[INFO] Pipeline Spark Streaming lancé. En attente de données Kafka...")

    # =================================================================
    # 8. Attente de la fin (streaming)
    # =================================================================
    spark.streams.awaitAnyTermination()



if __name__ == "__main__":
    main()
