import sys
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
SILVER_DB = args["SILVER_DB"]
SILVER_TABLE = args["SILVER_TABLE"]
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
            clean = F.trim(F.regexp_replace(F.col(c), r"\s+", " "))
            df = df.withColumn(
                c,
                F.when(
                    clean.isNull()
                    | (clean == "")
                    | F.lower(clean).isin(NULL_TOKENS),
                    None,
                ).otherwise(clean),
            )
    return df


def safe_double(col):
    return F.regexp_replace(col, ",", ".").cast("double")


def safe_bigint(col):
    return (
        F.regexp_replace(col, ",", ".")
         .cast("double")
         .cast("bigint")
    )


def add_p_source_date(df):
    """
    Extrai data do nome do arquivo:
    QBR_Production_2026_1_8_trips.csv  -> 2026-01-08
    """
    return (
        df.withColumn("_source_file", F.input_file_name())
          .withColumn(
              "p_source_date_raw",
              F.regexp_extract(
                  F.col("_source_file"),
                  r"(\d{4}_\d{1,2}_\d{1,2})",
                  1,
              ),
          )
          .withColumn(
              "p_source_date",
              F.to_date(
                  F.col("p_source_date_raw"),
                  "yyyy_M_d",
              ),
          )
          .drop("p_source_date_raw")
    )


def delete_folder_markers(s3_uri):
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
        .option("inferSchema", "false")
        .csv(BRONZE_S3)
)

df = normalize_strings(df)

print(f"### Registros lidos do Bronze: {df.count()}")

# ===============================================================
# 5. CASTS — ALINHADOS AO SCHEMA ATHENA
# ===============================================================

df = (
    df
    # IDs
    .withColumn("drive_id",   F.col("drive_id").cast("bigint"))
    .withColumn("vehicle_id", F.col("vehicle_id").cast("bigint"))
    .withColumn("driver_id",  F.col("driver_id").cast("bigint"))

    # Datas
    .withColumn("start_drive", F.to_timestamp("start_drive", "dd/MM/yyyy HH:mm:ss"))
    .withColumn("end_drive",   F.to_timestamp("end_drive", "dd/MM/yyyy HH:mm:ss"))

    # Localização
    .withColumn("start_location", F.col("start_location"))
    .withColumn("end_location",   F.col("end_location"))
    .withColumn("start_latitude",  safe_double(F.col("start_latitude")))
    .withColumn("start_longitude", safe_double(F.col("start_longitude")))
    .withColumn("end_latitude",    safe_double(F.col("end_latitude")))
    .withColumn("end_longitude",   safe_double(F.col("end_longitude")))

    # Métricas
    .withColumn("drive_duration", safe_double(F.col("drive_duration")))
    .withColumn("idle_duration",  safe_double(F.col("idle_duration")))
    .withColumn("mileage",        safe_double(F.col("mileage")))
    .withColumn("avg_speed",      safe_double(F.col("avg_speed")))

    # Eventos (BIGINT)
    .withColumn("turn1", safe_bigint(F.col("turn1")))
    .withColumn("turn2", safe_bigint(F.col("turn2")))
    .withColumn("turn3", safe_bigint(F.col("turn3")))
    .withColumn("break1", safe_bigint(F.col("break1")))
    .withColumn("break2", safe_bigint(F.col("break2")))
    .withColumn("break3", safe_bigint(F.col("break3")))
    .withColumn("acceleration1", safe_bigint(F.col("acceleration1")))
    .withColumn("acceleration2", safe_bigint(F.col("acceleration2")))
    .withColumn("acceleration3", safe_bigint(F.col("acceleration3")))
    .withColumn("speed_road1", safe_bigint(F.col("speed_road1")))
    .withColumn("speed_road2", safe_bigint(F.col("speed_road2")))
    .withColumn("speed_road3", safe_bigint(F.col("speed_road3")))

    # Energia (DOUBLE)
    .withColumn("energy_consumption", safe_double(F.col("energy_consumption")))
    .withColumn("energy_used",        safe_double(F.col("energy_used")))
    .withColumn("start_energy_level", safe_double(F.col("start_energy_level")))
    .withColumn("end_energy_level",   safe_double(F.col("end_energy_level")))
    .withColumn("start_soc",          safe_double(F.col("start_soc")))
    .withColumn("end_soc",            safe_double(F.col("end_soc")))

    # Sistemas
    .withColumn("breaking_system", safe_double(F.col("breaking_system")))
    .withColumn("gearbox",         F.col("gearbox"))
    .withColumn("engine",          F.col("engine"))
    .withColumn("clutch_system",   F.col("clutch_system"))
)

# ===============================================================
# 6. p_source_date (CORRETO)
# ===============================================================

df = add_p_source_date(df)

# ===============================================================
# 7. DEDUPLICAÇÃO TÉCNICA
# ===============================================================

w_drive = (
    Window
        .partitionBy("drive_id")
        .orderBy(F.col("_source_file").desc())
)

df = (
    df.withColumn("rn", F.row_number().over(w_drive))
      .filter((F.col("drive_id").isNull()) | (F.col("rn") == 1))
      .drop("rn")
)

df.persist(StorageLevel.MEMORY_AND_DISK)

# ===============================================================
# 8. ANTI-JOIN CONTRA SILVER
# ===============================================================

try:
    df_existing = (
        spark.read
            .parquet(SILVER_S3)
            .select("drive_id")
            .dropna()
            .distinct()
    )

    df = (
        df.alias("new")
          .join(df_existing.alias("old"), on="drive_id", how="left_anti")
    )
except Exception:
    pass

# ===============================================================
# 9. ESCRITA SILVER
# ===============================================================

(
    df.write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("p_source_date")
      .save(SILVER_S3)
)

# ===============================================================
# 10. FINALIZAÇÃO
# ===============================================================

spark.sql(f"MSCK REPAIR TABLE {SILVER_DB}.{SILVER_TABLE}")
delete_folder_markers(SILVER_S3)

job.commit()
