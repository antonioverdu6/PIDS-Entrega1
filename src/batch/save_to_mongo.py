
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import IntegerType, DoubleType

MONGO_URI = "mongodb://taxi-mongo:27017"
DB = "taxi_db"
COLL = "trips"
CSV_PATH = "/app/data/taxis.csv"

spark = (
    SparkSession.builder
    .appName("TaxiToMongoTyped")
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
    .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}/{DB}")
    .getOrCreate()
)



df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(CSV_PATH)
)

df2 = (
    df
    .withColumn("tpep_pickup_datetime",  to_timestamp("tpep_pickup_datetime",  "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime", "MM/dd/yyyy hh:mm:ss a"))
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance",  col("trip_distance").cast(DoubleType()))
    .withColumn("RatecodeID",     col("RatecodeID").cast(IntegerType()))
    .withColumn("PULocationID",   col("PULocationID").cast(IntegerType()))
    .withColumn("DOLocationID",   col("DOLocationID").cast(IntegerType()))
    .withColumn("payment_type",   col("payment_type").cast(IntegerType()))
    .withColumn("fare_amount",    col("fare_amount").cast(DoubleType()))
    .withColumn("extra",          col("extra").cast(DoubleType()))
    .withColumn("mta_tax",        col("mta_tax").cast(DoubleType()))
    .withColumn("tip_amount",     col("tip_amount").cast(DoubleType()))
    .withColumn("tolls_amount",   col("tolls_amount").cast(DoubleType()))
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType()))
    .withColumn("total_amount",   col("total_amount").cast(DoubleType()))
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType()))
)

df2.write \
    .format("mongodb") \
    .mode("append") \
    .option("spark.mongodb.connection.uri", "mongodb://taxi-mongo:27017") \
    .option("spark.mongodb.database", "taxi_db") \
    .option("spark.mongodb.collection", "trips") \
    .save()


spark.stop()
