# Contexto analítica Nube Microsoft Azure

Date: 29/06/2023
Status: Done
Type : Azure

### Contenido

- Creación de la infraestructura por medio de Azure.
    1. [Implementación.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
    2. [Recursos usados y explicación.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
    3. [Video explicativo.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21) 
    - [Código implementado para la solución.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        1. [Variables globales a ser usadas en el programa.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        2. [Configuración de los puntos de montaje.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        3. [Importación de librerías y creación del esquema de datos.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        4. [Extracción y transformación de los datos](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        5. [Carga de los datos en las capas raw y silver.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        6. [Detener procesos de spark streaming.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
- Implementación de la infraestructura en DataBricks.
    1. [Cambios en la implementación.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
    2. [Recursos usados y explicación.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
    3. [Video explicativo.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
    - [Código implementado para la solución.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        1. [Importación de librerías.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        2. [Variables globales a ser usadas en el programa.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        3. [Creación del esquema de datos.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        4. [Extracción, partición y carga de datos en capa raw.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        5. [Simulación del stream y carga de datos en capa silver.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)
        6. [Detener procesos de spark streaming.](https://www.notion.so/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3?pvs=21)

### Enlaces externos

<aside>
<img src="https://upload.wikimedia.org/wikipedia/commons/3/3e/Diagrams.net_Logo.svg" alt="https://upload.wikimedia.org/wikipedia/commons/3/3e/Diagrams.net_Logo.svg" width="15px" /> <a href="https://drive.google.com/file/d/14yFy4zCQs3M6ZA1pN64GY_o59eU-S-qF/view?usp=drive_link">Modelo C4: Arquitectura.</a>

</aside>

<aside>
<img src="https://arunpotti.files.wordpress.com/2021/12/microsoft_azure.svg_.png" alt="https://arunpotti.files.wordpress.com/2021/12/microsoft_azure.svg_.png" width="15px" /> Creación de la infraestructura por medio de Azure

</aside>

### 1. Implementación

El presente documento contiene en detalle la implementación de un proyecto de analítica completo haciendo uso de la arquitectura Data Lake House. Además, se muestra la integración de los sistemas de almacenamiento de datos con herramientas de visualización para la construcción de dashboards que soporten la toma de decisiones basadas en datos.

Principalmente se hará uso de Apache Kafka para el consumo de los datos de topicos definidos; Azure para el despliegue de la infraestructura LakeHouse y PowerBI para el consumo de los datos almacenados y la creación de reportes.

![Diagrama de contexto del sistema.](https://github.com/sapuertaf/AzureInfraestructure/blob/main/StreamingETLKafka/resources/C4ModelKafka.drawio.svg)

Diagrama de contexto del sistema.

### 2. Recursos usados y explicación.

A continuación, se explica brevemente la infraestructura creada y a ser usada:

- **Espacio de trabajo de Databricks:** Utilizado para la ejecución de jobs de PySpark en tiempo real para la ingesta, procesamiento y carga de datos. En este espacio de trabajo se crean notebooks (scripts) y clusters para la ejecución de trabajos.
- **Alcance secreto Databricks:** Utilizado para la autenticación con la cuenta de almacenamiento para habilitar la escritura y lectura en el Data Lake desde Databricks.
- **Cuenta de almacenamiento:** Utilizada para la creación del Data Lake. En esta se crean contenedores utilizados para almacenar objetos de datos en carpetas. Cada carpeta representa una tabla.
- **Key Vault (bóveda de llaves):** Utilizada para almacenar la clave primaria de la cuenta de almacenamiento.
- **Azure Synapse Analytics:** Utilizado para la generación de vistas materializadas que permiten vincular los datos almacenados en el Data Lake. Además, puede ser utilizado para la generación de consultas ad-hoc haciendo uso de lenguaje SQL.
- **Power BI:** Utilizado para la construcción del modelo relacional y la generación de dashboards con KPI’s para soportar la toma de decisiones basadas en datos.

### 3. Video explicativo

En este video se detalla la creación de la infraestructura para la implementación del proyecto recientemente descrito. 

[Contexto Azure.mp4](https://www.notion.so/ronald-notebook/Contexto-anal-tica-Nube-Microsoft-Azure-f2a6703d23f94e879cf73475bc1aa8e3#4cb71f36a27a448eb179f4bc5028c03a)

### Código implementado para la solución

### 1. Variables globales a ser usadas en el programa

En esta sección se presenta una lista de variables globales a ser usadas por el programa.

```python
RAW_PATH = "wasbs://raw@storagesergio9090.blob.core.windows.net"
MOUNT_POINT_RAW = "/mnt/raw"
RAW_CHECKPOINT_LOCATION = "/mnt/raw/check"
RAW_DATA_PATH = "/mnt/raw/test-data"
SILVER_PATH = "wasbs://silver@storagesergio9090.blob.core.windows.net"
MOUNT_POINT_SILVER = "/mnt/silver"
SILVER_CHECKPOINT_LOCATION = "/mnt/silver/check"
SILVER_DATA_PATH = "/mnt/silver/test-data"
DATABRICKS_SCOPE = "databricks-secret-scope1"
AZURE_SECRET_PATH = "fs.azure.account.key.storagesergio9090.blob.core.windows.net"
AZURE_SECRET_NAME = "data-lake-access-key"
```

La siguiente tabla explica la funcionalidad que cumple cada una de ellas.

| Variable | Funcionalidad |
| --- | --- |
| RAW_PATH | Url a contenedor raw de la cuenta de almacenamiento creada previamente en Azure Portal. |
| MOUNT_POINT_RAW | Dirección del punto de montaje interno de la capa raw. |
| RAW_CHECKPOINT_LOCATION | Dirección interna donde se guardara el checkpoint de ejecución de spark  streaming.  |
| RAW_DATA_PATH | Dirección interna donde se guardaran los datos de la capa raw.  |
| SILVER_PATH | Url a contenedor silver de la cuenta de almacenamiento creada previamente en Azure Portal. |
| MOUNT_POINT_SILVER | Dirección del punto de montaje interno de la capa silver. |
| SILVER_CHECKPOINT_LOCATION | Dirección interna donde se guardara el checkpoint de ejecución de spark  streaming.  |
| SILVER_DATA_PATH | Dirección interna donde se guardaran los datos de la capa silver. |
| DATABRICKS_SCOPE |  |
| AZURE_SECRET_PATH | Url al secreto (clave de acceso al datalake) almacenado en key vault.  |
| AZURE_SECRET_NAME | Nombre puesto al secreto (clave de acceso al datalake) almacenado en key vault. |

### 2. Configuración de los puntos de montaje

> Un punto de montaje es un directorio virtual que se crea en el sistema de archivos de Databricks (DBFS, por sus siglas en inglés) y que permite acceder y trabajar con datos almacenados en otros sistemas de almacenamiento, como Azure Blob Storage, Amazon S3 o sistemas de archivos Hadoop. Al crear un punto de montaje, se establece una conexión entre Databricks y el sistema de almacenamiento externo, lo que permite acceder a los datos como si estuvieran almacenados localmente en DBFS.
> 

En esta sección creamos y configuramos los puntos de montaje, con el propósito de que todos los datos que se almacenen en el DBFS de DataBricks se vean reflejados en la cuenta de almacenamiento (data lake) creada en Azure Portal.

```python
#configurando y montando puntos de montaje raw DataBricks - Azure
dbutils.fs.mount(
    source = RAW_PATH,
    mount_point = MOUNT_POINT_RAW,
    extra_configs = {AZURE_SECRET_PATH:dbutils.secrets.get(scope=DATABRICKS_SCOPE, key=AZURE_SECRET_NAME)}
)
#configurando y montando puntos de montaje silver DataBricks - Azure
dbutils.fs.mount(
    source = SILVER_PATH,
    mount_point = MOUNT_POINT_SILVER,
    extra_configs = {AZURE_SECRET_PATH:dbutils.secrets.get(scope=DATABRICKS_SCOPE, key=AZURE_SECRET_NAME)}
)
```

### 3. Importación de librerías y creación del esquema de datos.

```python
from pyspark.sql.types import BooleanType, StructType, StringType, TimestampType
from pyspark.sql.functions import *
```

```python
# Se crea el esquema para los datos que son publicado en el tópico de Kafka (se le da una estructura a los mensajes de entrada)
schema = StructType()\
         .add("timestamp", TimestampType())\
         .add("url", StringType())\
         .add("userURL", StringType())\
         .add("pageURL", StringType())\
         .add("isNewPage", BooleanType())\
         .add("geocoding", StructType()
            .add("countryCode2", StringType())
            .add("city", StringType())
            .add("latitude", StringType())
            .add("country", StringType())
            .add("longitude", StringType())
            .add("stateProvince", StringType())
            .add("countryCode3", StringType())
            .add("user", StringType())
            .add("namespace", StringType())
```

### 4. Extracción y transformación de los datos

```python
# Comando para cargar datos de cualquier tipo de fuente. Se utiliza el objeto Spark que permite la conexión hacia el cluster de Spark. (transformación: leer o suscribirse a un canal de kafka)
kafkaDF = (spark   # kafkaDF es un DataFrame en un objeto de python.
           .readStream # Función para conexión a un cluster de Kafka
           .option("kafka.bootstrap.servers", "server1.databricks.training:9092") # Servidor público para pruebas
           .option("subscribe", "en") # Sucripción a un canal con el nombre de "en"
           .format("kafka")  # Se define el formato tipo kafka
           .load() 
           )
```

```python
# Los DataFrame de Spark son inmutables, una vez creados no se pueden modificar (se debe sobreescribir o crear uno nuevo)
kafkaCleanDF = (kafkaDF
                .select(from_json(col("value").cast(StringType()),schema).alias("message")) # Ingresando dentro de value, transformar los datos (Type), se aplica el esquema desarrollado anteriormente y se le pone un alias con el nombre de "message"
                .select("message.*") # traer todas las columnas de la variable "mmessage"
               )
```

```python
myStreamName = "prueba_streaming"
display(kafkaCleanDF, streamName = myStreamName) # Crea una tabla en donde se pueden observar los resultados
```

```python
geocodingDF = (kafkaCleanDF
              .filter(col("geocoding.country").isNotNull()) # Se ingresa a la columna "geocoding_country" y se seleccionan las filas con este atributo no nulo
              .select("timestamp", "pageURL", "geocoding.countryCode2", "geocoding.city") # Me quedo con los atribudos que me importan para el análisis
              )
display(geocodingDF)
```

### 5. Carga de datos en las capas raw y silver

```python
# Guardar en capa raw (bronze)
(spark.readStream
  .format("kafka")  
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en, ja")
  .load()
  .withColumn("json", from_json(col("value").cast("string"), schema))
  .select(col("timestamp").alias("KafkaTimestamp"), col("json.*"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", RAW_CHECKPOINT_LOCATION)
  .outputMode("append")
  .queryName('bronze_stream')
  .start(RAW_DATA_PATH)
)
```

```python
# Guardar en capa silver
(spark.readStream
  .format("delta")
  .load(RAW_DATA_PATH)
  .select(col('KafkaTimestamp'), 
          # expr('left(comment,100) as Comments'), 
          col("namespace"),
          col("user"),
          when(col("geocoding.countryCode2").isNotNull(), col("geocoding.countryCode2")).otherwise("Unknown").alias("CountryCode"),\
          col("flag"),
          col("pageURL"))
  .writeStream
  .format("delta")
  .option("checkpointLocation", SILVER_CHECKPOINT_LOCATION)
  .outputMode("append")
  .queryName('silver_stream')
  .start(SILVER_DATA_PATH)
)
```

### 6. Detener procesos de spark streaming

```python
#Para detener los procesos de streaming
for s in spark.streams.active:
      s.stop()
```

---

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