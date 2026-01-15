import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Lista de tabelas Silver que DEVEM existir
tables = [
    "drivers",
    "groups",
    "schemes",
    "vehicles",
    "trips",
    "events"
]

database = "qstr_silver"

errors = []

for table in tables:
    try:
        df = spark.table(f"{database}.{table}")
        count = df.count()
        if count == 0:
            errors.append(f"Tabela {table} está vazia")
    except Exception as e:
        errors.append(f"Tabela {table} não encontrada: {str(e)}")
if errors:
    raise Exception("Checkpoint FAILED:\n" + "\n".join(errors))
print("Checkpoint OK: Silver pronta para consumo")
