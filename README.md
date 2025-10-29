# 🧠 Análisis de Casos Positivos de COVID-19 en Colombia (Apache Spark)

## 📘 Descripción
Procesamiento **Batch con Apache Spark** del dataset oficial **gt2j-8ykr.csv** del INS.  
Objetivo: limpiar, transformar y analizar casos de COVID-19 en Colombia, generando indicadores de contagios, mortalidad, recuperación y distribución por sexo, edad y departamento.

---

## 🧾 Dataset
- **Fuente:** [Datos Abiertos Colombia](https://www.datos.gov.co/resource/gt2j-8ykr.csv)  
- **Proveedor:** Instituto Nacional de Salud (INS)  
- **Cobertura:** Nacional  
- **Frecuencia:** Actualización diaria  
- **Columnas clave:**  
  `fecha_reporte_web`, `id_de_caso`, `departamento_nom`, `ciudad_municipio_nom`, `edad`, `sexo`, `estado`, `recuperado`, `fuente_tipo_contagio`, `ubicacion`, `fecha_inicio_sintomas`, `fecha_diagnostico`, `fecha_muerte`, `fecha_recuperado`.

---

## ⚙️ Tecnologías
- Apache Spark 3.5.6 (PySpark)  
- Python 3.8+  
- Ubuntu / VirtualBox  
- HDFS local (opcional)

---

## 🚀 Ejecución

1. **Descargar dataset:**
   ```bash
   wget https://www.datos.gov.co/resource/gt2j-8ykr.csv -O ~/casos_covid.csv
