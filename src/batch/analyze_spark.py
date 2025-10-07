from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour
import matplotlib.pyplot as plt

# =============================
# 1. Crear SparkSession
# =============================
spark = SparkSession.builder \
    .appName("AnalyzeTaxiData") \
    .master("local[*]") \
    .getOrCreate()

print("✅ SparkSession activa")

# =============================
# 2. Leer parquet limpio
# =============================
df = spark.read.parquet("/app/data/taxis_clean.parquet")
print("Número de filas limpias:", df.count())

# =============================
# 3. Viajes por hora
# =============================
df = df.withColumn("hour", hour("pickup_ts"))

trips_per_hour = df.groupBy("hour").count().orderBy("hour")
pandas_hour = trips_per_hour.toPandas()

plt.figure(figsize=(10,4))
plt.bar(pandas_hour["hour"], pandas_hour["count"])
plt.xlabel("Hora del día")
plt.ylabel("Número de viajes")
plt.title("Viajes por hora del día")
plt.savefig("/app/data/viajes_por_hora.png")
print("📊 Guardado: viajes_por_hora.png")

# =============================
# 4. Histograma de distancias
# =============================
pandas_dist = df.select("trip_distance").toPandas()

plt.figure(figsize=(8,4))
plt.hist(pandas_dist["trip_distance"], bins=20, edgecolor="black")
plt.xlabel("Distancia (millas)")
plt.ylabel("Frecuencia")
plt.title("Distribución de distancias")
plt.savefig("/app/data/hist_distancia.png")
print("📊 Guardado: hist_distancia.png")

# =============================
# 5. Histograma de duración
# =============================
pandas_dur = df.select("trip_duration_min").toPandas()

plt.figure(figsize=(8,4))
plt.hist(pandas_dur["trip_duration_min"], bins=20, edgecolor="black")
plt.xlabel("Duración (min)")
plt.ylabel("Frecuencia")
plt.title("Distribución de duración")
plt.savefig("/app/data/hist_duracion.png")
print("📊 Guardado: hist_duracion.png")

# =============================
# 6. Scatter distancia vs importe
# =============================
pandas_scatter = df.select("trip_distance", "total_amount").toPandas()

plt.figure(figsize=(8,6))
plt.scatter(pandas_scatter["trip_distance"], pandas_scatter["total_amount"], alpha=0.5)
plt.xlabel("Distancia (millas)")
plt.ylabel("Importe total ($)")
plt.title("Distancia vs Importe")
plt.savefig("/app/data/scatter_distancia_importe.png")
print("📊 Guardado: scatter_distancia_importe.png")

spark.stop()
print(">>> Fin del análisis")
