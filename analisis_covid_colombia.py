from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, count, avg, when, to_date, desc, isnan, lit
)

# 1️ Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Analisis-COVID19-Colombia") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️ Ruta local del archivo CSV descargado
#   (usa 'file:///' para lectura en Linux/Ubuntu dentro de la VM)
file_path = "file:///home/hadoop/casos_covid.csv"

# 3️⃣ Cargar dataset CSV con comillas y encabezados
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
print(f"Total de registros: {df.count():,}")

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

# Reemplazar valores vacíos o "sin dato" por NULL o "No reportado"
df = df.replace(["", " ", "Sin dato", "sin dato", "NA", "N/A"], None)

# Corregir valores nulos en columnas clave
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
# CONSULTAS ANALÍTICAS
# ============================================================

# 1️ Total de casos confirmados
print(" [1] Total de casos confirmados:")
df.select(count("*").alias("Total_Casos")).show()

# 2️ Distribución de casos por sexo
print(" [2] Distribución de casos por sexo:")
df.groupBy("sexo").count().orderBy(desc("count")).show()

# 3️ Casos por departamento (Top 10)
print(" [3] Casos por departamento (Top 10):")
df.groupBy("departamento_nom").count().orderBy(desc("count")).show(10, truncate=False)

# 4️ Promedio de edad por estado de salud
print(" [4] Promedio de edad por estado de salud:")
df.groupBy("estado").agg(avg("edad").alias("Edad_Promedio")).orderBy(desc("Edad_Promedio")).show(10, truncate=False)

# 5️ Evolución mensual de casos (por año/mes)
print(" [5] Evolución mensual de casos:")
df.groupBy("ANIO", "MES").count().orderBy("ANIO", "MES").show(12, truncate=False)

# 6️ Tasa de recuperación y mortalidad
total = df.count()
recuperados = df.filter(col("recuperado") == "Recuperado").count()
fallecidos = df.filter(col("estado") == "Fallecido").count()

tasa_recuperacion = (recuperados / total) * 100
tasa_mortalidad = (fallecidos / total) * 100

print(" [6] Indicadores nacionales:")
print(f"Tasa de recuperación: {tasa_recuperacion:.2f}%")
print(f"Tasa de mortalidad: {tasa_mortalidad:.2f}%")

# 7️⃣ Edad promedio por tipo de contagio (Top 5)
print(" [7] Edad promedio por fuente de contagio:")
df.groupBy("fuente_tipo_contagio") \
    .agg(avg("edad").alias("Edad_Promedio")) \
    .orderBy(desc("Edad_Promedio")) \
    .show(5, truncate=False)

# 8️ Distribución de casos por ubicación (Casa, Hospital, UCI, etc.)
print(" [8] Distribución por ubicación del paciente:")
df.groupBy("ubicacion").count().orderBy(desc("count")).show(10, truncate=False)

# 9️ Promedio de edad por sexo y estado
print(" [9] Edad promedio por sexo y estado:")
df.groupBy("sexo", "estado").agg(avg("edad").alias("Edad_Promedio")).orderBy("sexo").show(10, truncate=False)

# ============================================================
# 🗂️ GUARDAR RESULTADOS
# ============================================================

output_path = "file:///home/hadoop/resultados_covid.parquet"
df.write.mode("overwrite").parquet(output_path)
print(f"\n✅ Datos limpios guardados en {output_path}")

# Cerrar sesión
spark.stop()
print("\n🚀 Análisis completado con éxito.")
