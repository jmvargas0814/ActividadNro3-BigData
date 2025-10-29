from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, count, avg, to_date, desc

# 1️⃣ Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Analisis-COVID19-Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Ruta HDFS del archivo CSV
file_path = "hdfs://localhost:9000/Actividad3/gt2j-8ykr.csv"

# 3️⃣ Cargar dataset CSV con encabezados
df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True,
    quote='"',
    escape='"',
    multiLine=True
)

print("Dataset cargado correctamente")
df.printSchema()
print(f"Total de registros: {df.count():,}\n")

# ============================================================
# LIMPIEZA Y TRANSFORMACIÓN
# ============================================================

# Convertir columnas de fechas a tipo DATE
date_cols = [
    "fecha_reporte_web", "fecha_de_notificaci_n", "fecha_inicio_sintomas",
    "fecha_muerte", "fecha_diagnostico", "fecha_recuperado"
]
for c in date_cols:
    df = df.withColumn(c, to_date(col(c)))

# Reemplazar valores vacíos o "sin dato" por NULL
df = df.replace(["", " ", "Sin dato", "sin dato", "NA", "N/A"], None)

# Rellenar valores nulos en columnas clave
df = df.fillna({
    "sexo": "No reportado",
    "fuente_tipo_contagio": "Desconocido",
    "estado": "Sin estado",
    "recuperado": "No reportado",
    "departamento_nom": "Sin departamento",
    "ciudad_municipio_nom": "Sin ciudad"
})

# Crear columnas de año y mes
df = df.withColumn("ANIO", year(col("fecha_diagnostico")))
df = df.withColumn("MES", month(col("fecha_diagnostico")))

# ============================================================
# CONSULTAS ANALÍTICAS Y RESULTADOS EN CONSOLA
# ============================================================

# Total de casos confirmados
print("🔹 Total de casos confirmados")
df.select(count("*").alias("Total_Casos")).show()

# Casos por departamento (Top 10)
print("🔹 Casos por departamento (Top 10)")
df.groupBy("departamento_nom").count().orderBy(desc("count")).show(10, truncate=False)

# Promedio de edad por estado de salud
print("🔹 Promedio de edad por estado de salud")
df.groupBy("estado").agg(avg("edad").alias("Edad_Promedio")).show(10, truncate=False)

# Evolución mensual de casos
print("🔹 Evolución mensual de casos")
df.groupBy("ANIO", "MES").count().orderBy("ANIO", "MES").show(12, truncate=False)

# Cerrar sesión
spark.stop()
print("\n🚀 Análisis completado con éxito.")
