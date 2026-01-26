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
    ["JOB_NAME", "BRONZE_S3_PATH", "SILVER_DB", "SILVER_TABLE", "SILVER_S3_PATH"]
)

BRONZE_S3 = args["BRONZE_S3_PATH"].rstrip("/")
SILVER_S3 = args["SILVER_S3_PATH"].rstrip("/")
SILVER_DB = args["SILVER_DB"].strip()

# ===============================================================
# 2. CONTEXTOS
# ===============================================================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic")

spark.sql("SHOW DATABASES").show(200, False)
spark.sql("SHOW TABLES IN qstr_silver").show(200, False)
spark.sql("SELECT max(p_source_date) FROM qstr_silver.trips").show()
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
                    clean.isNull() | (clean == "") | F.lower(clean).isin(*NULL_TOKENS),
                    "sem dado",
                ).otherwise(clean),
            )
    return df

def add_p_source_date_from_filename(df):
    """
    Extrai p_source_date do nome do arquivo (robusto).
    Se não achar, fica NULL (não usa current_date pra não “inventar” 21).
    """
    df = df.withColumn("_source_file", F.input_file_name())

    date_str = F.regexp_extract(
        F.col("_source_file"),
        r"(\d{4}[_-]\d{1,2}[_-]\d{1,2})",
        1
    )
    date_str_norm = F.regexp_replace(date_str, "_", "-")

    return df.withColumn(
        "p_source_date",
        F.when(
            F.length(date_str_norm) > 0,
            F.to_date(date_str_norm, "yyyy-M-d")
        ).otherwise(F.lit(None).cast("date"))
    )

def delete_old_partitions(base_s3_path, keep_date_str):
    """
    Remove todas as partições p_source_date diferentes da data mantida (keep_date_str)
    """
    p = urlparse(base_s3_path)
    s3 = boto3.client("s3")
    base_prefix = p.path.lstrip("/").rstrip("/") + "/"

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=p.netloc, Prefix=base_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            part_prefix = cp["Prefix"]
            if f"p_source_date={keep_date_str}" not in part_prefix:
                objs = s3.list_objects_v2(Bucket=p.netloc, Prefix=part_prefix).get("Contents", [])
                if objs:
                    s3.delete_objects(
                        Bucket=p.netloc,
                        Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
                    )

def delete_folder_markers(s3_uri):
    """
    Remove arquivos *_$folder$ (folder markers)
    """
    p = urlparse(s3_uri)
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=p.netloc, Prefix=p.path.lstrip("/")):
        markers = [
            {"Key": o["Key"]}
            for o in page.get("Contents", [])
            if o["Key"].endswith("_$folder$")
        ]
        if markers:
            s3.delete_objects(Bucket=p.netloc, Delete={"Objects": markers})

# ===============================================================
# 4. D-1 OFICIAL = MAX(p_source_date) DO TRIPS (SILVER CATALOG)
# ===============================================================
df_trips = spark.table(f"{SILVER_DB}.trips").select("p_source_date")

d1_oficial = (
    df_trips.filter(F.col("p_source_date").isNotNull())
            .agg(F.max("p_source_date").alias("max_date"))
            .collect()[0]["max_date"]
)

if d1_oficial is None:
    raise Exception(f"Não consegui obter D-1: {SILVER_DB}.trips não tem p_source_date válido.")

print(f"### D-1 OFICIAL (base trips): {d1_oficial} ###")

# ===============================================================
# 5. LEITURA BRONZE (DRIVERS)
# ===============================================================
df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")
    .csv(BRONZE_S3)
)

df = normalize_strings(df)

df = (
    df.withColumn("driver_id", F.col("driver_id").cast("bigint"))
      .withColumn("group_id", F.col("group_id").cast("bigint"))
      .withColumn("worker_id", F.col("worker_id").cast("bigint"))
)

# tenta extrair data do arquivo (se falhar, fica NULL)
df = add_p_source_date_from_filename(df)

# ===============================================================
# 6. FILTRO D-1 + DEDUP (DIMENSIONAL)
# ===============================================================
# mantém somente registros cujo arquivo indica o D-1 (quando houver)
df_d1 = df.filter(F.col("p_source_date") == F.lit(d1_oficial))

# se não veio nada (ex.: nome do arquivo não tem data), a gente NÃO “inventa”:
# gera erro pra você ver o problema no bronze imediatamente
if df_d1.rdd.isEmpty():
    raise Exception(
        f"Nenhum registro de drivers com p_source_date={d1_oficial} extraído do nome do arquivo. "
        "Verifique o padrão do nome dos arquivos no BRONZE drivers."
    )

# dedup: 1 linha por driver_id (pega o “mais recente” por arquivo)
w_driver = Window.partitionBy("driver_id").orderBy(F.col("_source_file").desc())

df_d1 = (
    df_d1.withColumn("rn", F.row_number().over(w_driver))
         .filter(F.col("rn") == 1)
         .drop("rn")
)

# força p_source_date oficial e remove coluna técnica
df_final = (
    df_d1.withColumn("p_source_date", F.lit(d1_oficial))
         .drop("_source_file")
)

# ===============================================================
# 7. LIMPEZA S3 (mantém só D-1) + folder markers
# ===============================================================
delete_folder_markers(SILVER_S3)
delete_old_partitions(SILVER_S3, str(d1_oficial))

# ===============================================================
# 8. ESCRITA SILVER — SOMENTE D-1
# ===============================================================
(
    df_final.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("p_source_date")
        .save(SILVER_S3)
)

# ===============================================================
# 9. GLUE CATALOG
# ===============================================================
glue = boto3.client("glue")

def glue_type(dt):
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
    glue.get_table(DatabaseName=args["SILVER_DB"], Name=args["SILVER_TABLE"])
except glue.exceptions.EntityNotFoundException:
    glue.create_table(
        DatabaseName=args["SILVER_DB"],
        TableInput={
            "Name": args["SILVER_TABLE"],
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
            "PartitionKeys": [{"Name": "p_source_date", "Type": "date"}],
            "StorageDescriptor": {
                "Columns": [
                    {"Name": f.name, "Type": glue_type(f.dataType.simpleString())}
                    for f in df_final.schema.fields
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

spark.sql(f"MSCK REPAIR TABLE {args['SILVER_DB']}.{args['SILVER_TABLE']}")
delete_folder_markers(SILVER_S3)

print(f"### SILVER DRIVERS FINALIZADO — mantida apenas p_source_date={d1_oficial} ###")
job.commit()
