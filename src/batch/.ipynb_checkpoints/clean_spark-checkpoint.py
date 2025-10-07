from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round

# =============================
# 1. Crear SparkSession
# =============================
spark = SparkSession.builder \
    .appName("CleanTaxiData") \
    .master("local[*]") \
    .getOrCreate()

print("✅ SparkSession activa")

# =============================
# 2. Leer CSV original
# =============================
input_path = "/app/data/taxis.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

print("Número de filas originales:", df.count())

# =============================
# 3. Convertir fechas y calcular duración
# =============================
df = df.withColumn(
    "pickup_ts",
    unix_timestamp(col("tpep_pickup_datetime"), "MM/dd/yyyy hh:mm:ss a")
).withColumn(
    "dropoff_ts",
    unix_timestamp(col("tpep_dropoff_datetime"), "MM/dd/yyyy hh:mm:ss a")
).withColumn(
    "trip_duration_min",
    round((col("dropoff_ts") - col("pickup_ts")) / 60.0, 2)
)

# =============================
# 4. Filtrar viajes inválidos
# =============================
df_clean = df.filter(
    (col("trip_distance") > 0) &
    (col("total_amount") > 0) &
    (col("trip_duration_min") > 0)
)

print("Número de filas tras limpieza:", df_clean.count())

# =============================
# 5. Guardar datos limpios
# =============================
output_path = "/app/data/taxis_clean.parquet"
df_clean.write.mode("overwrite").parquet(output_path)

print("✅ Datos limpios guardados en:", output_path)

spark.stop()
print(">>> Fin del script")
