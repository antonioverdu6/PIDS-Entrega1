import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_csv
from pymongo import MongoClient  

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC     = os.getenv("TOPIC", "taxi_trips")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://taxi-mongo:27017")
DB        = os.getenv("MONGO_DB", "taxi_db")
COLL      = os.getenv("MONGO_COLL", "trips")  
spark = (
    SparkSession.builder
        .appName("TaxiStreamToMongo")
        # Forzar carga de los jars de Kafka
        .config(
            "spark.jars",
            "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
            "/opt/spark/jars/kafka-clients-3.5.1.jar,"
            "/opt/spark/jars/commons-pool2-2.11.1.jar"
        )
        # (opcional) también por packages, redundante pero seguro
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(" SparkSession Streaming iniciada")


# Esquema en DDL (STRING)
schema_str = """
VendorID INT,
tpep_pickup_datetime STRING,
tpep_dropoff_datetime STRING,
passenger_count INT,
trip_distance DOUBLE,
RatecodeID INT,
store_and_fwd_flag STRING,
PULocationID INT,
DOLocationID INT,
payment_type INT,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
congestion_surcharge DOUBLE
"""

# 1) Kafka -> DataFrame streaming
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("subscribe", TOPIC)
         .option("startingOffsets", "latest")   # mejor 'latest' en producción con checkpoint
         .load()
)

# 2) Parsear CSV del campo 'value'
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS value")
       .select(from_csv(col("value"), schema_str).alias("r"))
       .select("r.*")
)

# 3) Guardar cada micro-batch en Mongo con PyMongo
def save_batch_to_mongo(df, epoch_id):
    rows = [r.asDict(recursive=True) for r in df.collect()]
    if not rows:
        return
    client = MongoClient(MONGO_URI)
    client[DB][COLL].insert_many(rows, ordered=False)
    client.close()

query = (
    parsed.writeStream
          .outputMode("append")
          .foreachBatch(save_batch_to_mongo)
          .option("checkpointLocation", "/tmp/checkpoints/taxi_trips")  # no borrar entre reinicios
          .start()
)

query.awaitTermination()
