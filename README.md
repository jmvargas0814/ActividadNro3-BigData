# 🧠 Análisis de Casos Positivos de COVID-19 en Colombia (Apache Spark)

## 📘 Descripción
Proyecto de procesamiento **Batch con Apache Spark**, utilizando el dataset oficial **gt2j-8ykr.csv** del Instituto Nacional de Salud (INS).  
El objetivo es realizar limpieza, transformación y análisis exploratorio de los casos de COVID-19 en Colombia.

---

## 🧾 Dataset
- **Fuente:** [Datos Abiertos de Colombia](https://www.datos.gov.co/resource/gt2j-8ykr.csv)
- **Proveedor:** Instituto Nacional de Salud (INS)
- **Cobertura:** Nacional  
- **Frecuencia de actualización:** Diaria  
- **Columnas relevantes:**
  - fecha_reporte_web
  - id_de_caso
  - departamento_nom
  - ciudad_municipio_nom
  - edad
  - sexo
  - estado
  - recuperado
  - fuente_tipo_contagio
  - ubicación
  - fechas de diagnóstico, síntomas, muerte y recuperación

---

## ⚙️ Tecnologías
- **Apache Spark 3.5.6**
- **Python 3.8+**
- **Ubuntu / VirtualBox**

---

## 🚀 Ejecución

1. Descargar el dataset oficial:
   ```bash
   wget casos_covid.csv
