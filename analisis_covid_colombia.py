from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, avg, when, to_date, desc

# 1️ Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Analisis-COVID19-Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️ Definir ruta local (archivo descargado con wget)
file_path = "file:///home/vboxuser/casos_covid.csv"

# 3️⃣ Cargar dataset CSV con comillas y encabezados
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True,
    quote='"',
    escape='"',
    multiLine=True
)

print("\n=== Dataset cargado correctamente ===")
df.printSchema()
print(f"Total de registros cargados: {df.count():,}")

# 4️ Limpieza básica
df = df.na.drop(subset=["fecha_diagnostico", "departamento_nom", "sexo"])

# Convertir a tipo fecha las columnas principales
df = df.withColumn("fecha_diagnostico", to_date(col("fecha_diagnostico")))
df = df.withColumn("fecha_reporte_web", to_date(col("fecha_reporte_web")))
df = df.withColumn("fecha_muerte", to_date(col("fecha_muerte")))
df = df.withColumn("fecha_recuperado", to_date(col("fecha_recuperado")))

# Crear columnas de año y mes
df = df.withColumn("ANIO", year(col("fecha_diagnostico")))
df = df.withColumn("MES", month(col("fecha_diagnostico")))

print("\n=== Limpieza y transformación completadas ===")

# ============================================================
# CONSULTAS PRINCIPALES
# ============================================================

# 1️ Total de casos confirmados
print(" [1] Total de casos confirmados:")
df.select(count("*").alias("Total_Casos")).show()

# 2️ Casos por estado (Recuperado, Fallecido, Leve, etc.)
print(" [2] Casos por estado:")
df.groupBy("estado").count().orderBy(desc("count")).show(10, truncate=False)

# 3️ Casos por departamento (Top 10)
print(" [3] Casos por departamento (Top 10):")
df.groupBy("departamento_nom").count().orderBy(desc("count")).show(10, truncate=False)

# 4️ Promedio de edad por departamento (Top 10)
print(" [4] Promedio de edad por departamento (Top 10):")
df.groupBy("departamento_nom").avg("edad").orderBy(desc("avg(edad)")).show(10, truncate=False)

# 5️ Evolución mensual de casos (año/mes)
print(" [5] Evolución mensual de casos:")
df.groupBy("ANIO", "MES").count().orderBy("ANIO", "MES").show(12, truncate=False)

# ============================================================
# Guardar resultados en formato Parquet (procesamiento limpio)
# ============================================================
output_path = "file:///home/vboxuser/resultados_covid19.parquet"
df.write.mode("overwrite").parquet(output_path)

print(f" Resultados procesados guardados en: {output_path}")

# 6️ Finalizar sesión
spark.stop()
print(" Análisis completado con éxito.")
