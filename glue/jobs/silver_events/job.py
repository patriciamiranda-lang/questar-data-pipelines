import sys
import re
import boto3
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# ===============================================================
# 1. PARÂMETROS
# ===============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_S3_PATH",
        "SILVER_DB",
        "SILVER_TABLE",
        "SILVER_S3_PATH",
    ]
)

BRONZE_S3 = args["BRONZE_S3_PATH"].rstrip("/")
SILVER_S3 = args["SILVER_S3_PATH"].rstrip("/")

# ===============================================================
# 2. CONTEXTO
# ===============================================================

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

# ===============================================================
# 3. FUNÇÕES AUXILIARES
# ===============================================================

NULL_TOKENS = {"null", "none", "nan", "n/a", "na", "-"}


def normalize_strings(df):
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(
                c,
                F.when(
                    F.trim(F.col(c)).isin(*NULL_TOKENS) | F.col(c).isNull(),
                    None,
                ).otherwise(F.trim(F.col(c))),
            )
    return df


def add_p_source_date_with_fallback(df):
    """
    p_source_date:
    - data do nome do arquivo (YYYY_MM_DD), quando existir
    - fallback para data de ingestão (current_date)
    """
    return (
        df.withColumn("_source_file", F.input_file_name())
          .withColumn(
              "p_source_date",
              F.coalesce(
                  F.to_date(
                      F.regexp_extract(
                          F.col("_source_file"),
                          r"(\d{4}_\d{2}_\d{2})",
                          1,
                      ),
                      "yyyy_MM_dd",
                  ),
                  F.current_date(),
              ),
          )
    )


def delete_old_partitions(base_s3_path, keep_date):
    """
    Remove todas as partições p_source_date diferentes da data mais recente
    """
    p = urlparse(base_s3_path)
    s3 = boto3.client("s3")

    base_prefix = p.path.lstrip("/").rstrip("/") + "/"

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=p.netloc,
        Prefix=base_prefix,
        Delimiter="/",
    ):
        for cp in page.get("CommonPrefixes", []):
            part_prefix = cp["Prefix"]
            if f"p_source_date={keep_date}" not in part_prefix:
                objs = s3.list_objects_v2(
                    Bucket=p.netloc,
                    Prefix=part_prefix,
                ).get("Contents", [])

                if objs:
                    s3.delete_objects(
                        Bucket=p.netloc,
                        Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
                    )


def delete_folder_markers(s3_uri):
    """
    Remove arquivos *_$folder$ (folder markers legados do Hadoop)
    """
    p = urlparse(s3_uri)
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=p.netloc,
        Prefix=p.path.lstrip("/"),
    ):
        markers = [
            {"Key": o["Key"]}
            for o in page.get("Contents", [])
            if o["Key"].endswith("_$folder$")
        ]

        if markers:
            s3.delete_objects(
                Bucket=p.netloc,
                Delete={"Objects": markers},
            )

# ===============================================================
# 4. LEITURA BRONZE
# ===============================================================

df = (
    spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(BRONZE_S3)
)

df = normalize_strings(df)

print(f"### Registros lidos do Bronze: {df.count()}")

# ===============================================================
# 5. CASTS + NORMALIZAÇÕES
# ===============================================================

df = (
    df.withColumn("event_id", F.col("event_id").cast("bigint"))
      .withColumn("vehicle_id", F.col("vehicle_id").cast("bigint"))
      .withColumn("scheme_id", F.col("scheme_id").cast("bigint"))
      .withColumn("drive_id", F.col("drive_id").cast("bigint"))
      .withColumn("start_latitude", F.col("start_latitude").cast("double"))
      .withColumn("start_longitude", F.col("start_longitude").cast("double"))
      .withColumn("end_latitude", F.col("end_latitude").cast("double"))
      .withColumn("end_longitude", F.col("end_longitude").cast("double"))
      .withColumn("direction", F.col("direction").cast("double"))
      .withColumn("speed", F.col("speed").cast("double"))
      .withColumn("max_speed", F.col("max_speed").cast("double"))
      .withColumn(
          "start_mileage",
          F.regexp_replace(F.col("start_mileage"), ",", ".").cast("double"),
      )
      .withColumn(
          "end_mileage",
          F.regexp_replace(F.col("end_mileage"), ",", ".").cast("double"),
      )
      .withColumn(
          "start_time",
          F.coalesce(
              F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss.SSS"),
              F.to_timestamp("start_time", "yyyy-MM-dd HH:mm:ss"),
              F.to_timestamp("start_time", "dd/MM/yyyy HH:mm:ss"),
              F.to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss"),
              F.to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          ),
      )
      .withColumn(
          "end_time",
          F.coalesce(
              F.to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss.SSS"),
              F.to_timestamp("end_time", "yyyy-MM-dd HH:mm:ss"),
              F.to_timestamp("end_time", "dd/MM/yyyy HH:mm:ss"),
              F.to_timestamp("end_time", "yyyy-MM-dd'T'HH:mm:ss"),
              F.to_timestamp("end_time", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
          ),
      )
)

# ===============================================================
# 6. p_source_date (COM FALLBACK)
# ===============================================================

df = add_p_source_date_with_fallback(df)

print("### Datas encontradas (após fallback):")
df.select("p_source_date").distinct().orderBy("p_source_date").show(100, False)

# ===============================================================
# 7. DEDUPLICAÇÃO TÉCNICA — EVENT_ID
# ===============================================================

w_event = (
    Window
        .partitionBy("event_id")
        .orderBy(
            F.col("p_source_date").desc(),
            F.col("_source_file").desc()
        )
)

df = (
    df.withColumn("rn", F.row_number().over(w_event))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# ===============================================================
# 7.1 IDENTIFICA DATA MAIS RECENTE
# ===============================================================

latest_date = (
    df.select(F.max("p_source_date").alias("max_date"))
      .collect()[0]["max_date"]
)

# ===============================================================
# 7.2 LIMPA PARTIÇÕES ANTIGAS + FOLDER MARKERS
# ===============================================================

delete_folder_markers(SILVER_S3)

# ===============================================================
# 8. ESCRITA SILVER — SOMENTE DATA MAIS RECENTE
# ===============================================================

(
    df.write \
      .mode("overwrite") \
      .format("parquet") \
      .partitionBy("p_source_date") \
      .save(SILVER_S3)
)

# ===============================================================
# 9. FINALIZAÇÃO
# ===============================================================

spark.sql(f"MSCK REPAIR TABLE {args['SILVER_DB']}.{args['SILVER_TABLE']}")
delete_folder_markers(SILVER_S3)

print(
    f"### silver_events FINALIZADO — MANTIDA APENAS p_source_date={latest_date} (sem _$folder$) ###"
)

job.commit()
