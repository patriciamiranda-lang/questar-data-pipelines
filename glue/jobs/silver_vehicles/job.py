import sys
import boto3
from urllib.parse import urlparse

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    ],
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


def add_partition_date_with_fallback(df):
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
                        Delete={
                            "Objects": [{"Key": o["Key"]} for o in objs]
                        },
                    )


def delete_folder_markers(s3_uri):
    p = urlparse(s3_uri)
    s3 = boto3.client("s3")

    objs = s3.list_objects_v2(
        Bucket=p.netloc,
        Prefix=p.path.lstrip("/"),
    ).get("Contents", [])

    markers = [{"Key": o["Key"]} for o in objs if o["Key"].endswith("_$folder$")]

    if markers:
        s3.delete_objects(
            Bucket=p.netloc,
            Delete={"Objects": markers},
        )

# ===============================================================
# 4. LEITURA BRONZE (VEHICLES)
# ===============================================================

df = (
    spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(BRONZE_S3)
)

# ===============================================================
# 5. TRANSFORMAÇÕES
# ===============================================================

df = normalize_strings(df)

df = (
    df.withColumn("vehicle_id", F.col("vehicle_id").cast("bigint"))
      .withColumn("group_id", F.col("group_id").cast("bigint"))
      .withColumn("unit_serial_number", F.col("unit_serial_number").cast("bigint"))
)

# ===============================================================
# 6. p_source_date (COM FALLBACK)
# ===============================================================

df = add_partition_date_with_fallback(df)

# ===============================================================
# 6.1 DEDUPLICAÇÃO — 1 VEHICLE POR vehicle_id
# ===============================================================

w_vehicle = (
    Window
        .partitionBy("vehicle_id")
        .orderBy(F.col("p_source_date").desc())
)

df = (
    df.withColumn("rn", F.row_number().over(w_vehicle))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

# ===============================================================
# 6.2 IDENTIFICA DATA MAIS RECENTE
# ===============================================================

latest_date = (
    df.select(F.max("p_source_date").alias("max_date"))
      .collect()[0]["max_date"]
)

# ===============================================================
# 6.3 LIMPA PARTIÇÕES ANTIGAS NO S3
# ===============================================================

delete_old_partitions(SILVER_S3, latest_date)

# ===============================================================
# 7. ESCRITA SILVER — SOMENTE DATA MAIS RECENTE
# ===============================================================

(
    df.write
      .mode("overwrite")
      .format("parquet")
      .partitionBy("p_source_date")
      .save(SILVER_S3)
)

# ===============================================================
# 8. GLUE CATALOG
# ===============================================================

glue = boto3.client("glue")


def spark_to_glue_type(dt):
    return {
        "string": "string",
        "bigint": "bigint",
        "int": "int",
        "double": "double",
        "float": "float",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "date": "date",
    }.get(dt, "string")


try:
    glue.get_table(DatabaseName=SILVER_DB, Name=SILVER_TABLE)
except glue.exceptions.EntityNotFoundException:
    glue.create_table(
        DatabaseName=SILVER_DB,
        TableInput={
            "Name": SILVER_TABLE,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
            "PartitionKeys": [{"Name": "p_source_date", "Type": "date"}],
            "StorageDescriptor": {
                "Columns": [
                    {
                        "Name": f.name,
                        "Type": spark_to_glue_type(
                            f.dataType.simpleString()
                        ),
                    }
                    for f in df.schema.fields
                    if f.name != "p_source_date"
                ],
                "Location": SILVER_S3,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
        },
    )

# ===============================================================
# 9. FINALIZAÇÃO PADRÃO SILVER
# ===============================================================

spark.sql(f"MSCK REPAIR TABLE {SILVER_DB}.{SILVER_TABLE}")
delete_folder_markers(SILVER_S3)

print(
    f"### silver_vehicles FINALIZADO — MANTIDA APENAS p_source_date={latest_date} ###"
)

job.commit()
