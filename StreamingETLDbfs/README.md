# Contexto analítica Nube Microsoft Azure

[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)

Date: 29/06/2023

### Contenido

- Implementación de la infraestructura en DataBricks.
    1. [Cambios en la implementación.](https://github.com/sapuertaf/AzureInfraestructure#1-cambios-en-la-implementaci%C3%B3n)
    2. [Recursos usados y explicación.](https://github.com/sapuertaf/AzureInfraestructure#2-recursos-usados-y-explicaci%C3%B3n-1)
    3. [Video explicativo.](https://github.com/sapuertaf/AzureInfraestructure#3-video-explicativo-1)
    - [Código implementado para la solución.](https://github.com/sapuertaf/AzureInfraestructure#c%C3%B3digo-implementado-para-la-soluci%C3%B3n-1)
        1. [Importación de librerías.](https://github.com/sapuertaf/AzureInfraestructure#1-importaci%C3%B3n-de-librer%C3%ADas)
        2. [Variables globales a ser usadas en el programa.](https://github.com/sapuertaf/AzureInfraestructure#2-variables-globales-a-ser-usadas-en-el-programa)
        3. [Creación del esquema de datos.](https://github.com/sapuertaf/AzureInfraestructure#3-creaci%C3%B3n-del-esquema-de-datos)
        4. [Extracción, partición y carga de datos en capa raw.](https://github.com/sapuertaf/AzureInfraestructure#4-extracci%C3%B3n-partici%C3%B3n-y-carga-de-datos-en-capa-raw)
        5. [Simulación del stream y carga de datos en capa silver.](https://github.com/sapuertaf/AzureInfraestructure#5-simulaci%C3%B3n-del-stream-y-carga-de-datos-en-capa-silver)
        6. [Detener procesos de spark streaming.](https://github.com/sapuertaf/AzureInfraestructure#6-detener-procesos-de-spark-streaming-1)

### Enlaces externos

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/3/3e/Diagrams.net_Logo.svg" alt="https://upload.wikimedia.org/wikipedia/commons/3/3e/Diagrams.net_Logo.svg" width="15px" /> <a href="https://drive.google.com/file/d/14yFy4zCQs3M6ZA1pN64GY_o59eU-S-qF/view?usp=drive_link">Modelo C4: Arquitectura.</a>

</aside>

<br><br>

<aside>
<img src="https://avatars.githubusercontent.com/u/4998052?s=280&v=4" alt="https://avatars.githubusercontent.com/u/4998052?s=280&v=4" width="15px" /> Implementación de la infraestructura en DataBricks

</aside>

### 1. Cambios en la implementación.

En lugar de usar Azure para el despliegue del LakeHouse, haremos uso de la plataforma Databricks y su sistema de archivos (DBFS). Esto implica el almacenamiento de los datos ya no en la cuenta de almacenamiento de Azure, si no, el almacenamiento de los datos dentro del sistema de archivos de Databricks.  

Principalmente se hará uso de Apache Kafka para el consumo de los datos de topicos definidos; Databricks Notebooks para el procesamiento y carga en el DBFS de los datos entrantes de los tópicos Kafka y el DBFS para el almacenamiento de los mismos.

![Diagrama de componentes del sistema: Espacio de trabajo de Databricks.](https://github.com/sapuertaf/AzureInfraestructure/blob/main/StreamingETLDbfs/resources/C4ModelDbfs.drawio.svg)

Diagrama de componentes del sistema: Espacio de trabajo de Databricks.

### 2. Recursos usados y explicación.

A continuación, se explica brevemente los recursos usados y su función.

- Databricks clusters: Usado para la asignación de recursos y la ejecución de las tareas de los Notebooks.
- Databricks Notebooks: Utilizado para la ejecución de jobs de PySpark en tiempo real para la ingesta, procesamiento y carga de datos.
- Databricks File System (DBFS): Usado para el almacenamiento y consulta de los datos.

### 3. Video explicativo

En este video se detalla la creación de la infraestructura para la implementación del proyecto recientemente descrito. 

[Implementación Infraestructura Databricks.mp4](https://www.notion.so/ronald-notebook/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3#fe4eb07e6e7f44dfac5cae84ab81cfab)

### Código implementado para la solución

### 1. Importación de librerías

```python
from pyspark.sql.types import IntegerType, StringType, StructType, FloatType
from pyspark.sql.functions import *
```

### 2. Variables globales a ser usadas en el programa

```python
#variables globales a usar en el programa
CSV_DATA_PATH = "dbfs:/FileStore/tables/walmart_data.csv"
PARTITIONS_NUMBER = 30 #numero de archivos a partir el csv de walmart 
RAW_PATH = "dbfs:/FileStore/raw/"
SILVER_PATH = "dbfs:/FileStore/silver/"
SILVER_CHECKPOINT_LOCATION = "dbfs:/FileStore/silver/check"
```

| Variable | Funcionalidad |
| --- | --- |
| CSV_DATA_PATH  | Ruta a carpeta dentro del DBFS donde se encuentra almacenado el CSV de entrada. |
| PARTITIONS_NUMBER  | Numero de archivos en los que se partira el CSV de entrada. |
| RAW_PATH  | Ruta a carpeta raw dentro del DBFS. |
| SILVER_PATH  | Ruta a carpeta silver dentro del DBFS. |
| SILVER_CHECKPOINT_LOCATION  | Dirección interna donde se guardara el checkpoint de ejecución de spark streaming.  |

### 3. Creación del esquema de datos

```python
schema = StructType()\
         .add("store",IntegerType())\
         .add("Date",StringType())\
         .add("weekly_sales",FloatType())\
         .add("is_holiday",IntegerType())\
         .add("temperature",FloatType())\
         .add("fuel_price",FloatType())\
         .add("cpi",FloatType())\
         .add("unemployment",FloatType())
```

### 4. Extracción, partición y carga de datos en capa raw

```python
#leer data de CSV de walmart sales
df_walmart = spark.read\
                  .format("csv")\
                  .option("header",True)\
                  .schema(schema)\
                  .load(CSV_DATA_PATH)
```

```python
#partir el conjunto de datos de walmart en n partes
partition_data = df_walmart.repartition(PARTITIONS_NUMBER)
```

```python
#guardando las n particiones del conjunto de datos en la capa raw
partition_data.write\
              .format("parquet")\
              .option("header",True)\
              .save(RAW_PATH)
```

### 5. Simulación del stream y carga de datos en capa silver

```python
#simulo el proceso en streaming que va tomando un archivo con cada ejecución
stream = spark.readStream\
              .format("parquet")\
              .schema(schema)\
              .option("header",True)\
              .option("ignoreLeadingWhiteSpace",True)\
              .option("mode","dropMalFormed")\
              .option("maxFilesPerTrigger",1)\
              .load(RAW_PATH)
```

```python
#escribo en la capa silver los datos tomados por medio del streaming
stream.writeStream\
      .option("checkPointLocation",SILVER_CHECKPOINT_LOCATION)\
      .format("parquet")\
      .outputMode("append")\
      .queryName("silver_stream")\
      .start(SILVER_PATH)
```

### 6. Detener procesos de spark streaming

```python
#para detener los procesos de streaming
for s in spark.streams.active:
      s.stop()
```