import json
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

BRONZE_PREFIX = "bronze/"
SOURCE_PREFIXES = (
    "QBR_Production_drivers/",
    "QBR_Production_events/",
    "QBR_Production_group/",
    "QBR_Production_schemes/",
    "QBR_Production_trips/",
    "QBR_Production_vehicles/",
)

def lambda_handler(event, context):
    print("===== Evento recebido =====")
    print(json.dumps(event, indent=2))

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])

    print(f"Bucket: {bucket}")
    print(f"Key recebida: {key}")
    
    if key.startswith(BRONZE_PREFIX):
        print("Arquivo já está em bronze. Ignorando.")
        return {"statusCode": 200}

    if not key.startswith(SOURCE_PREFIXES):
        print("Arquivo fora do escopo de ingestão. Ignorando.")
        return {"statusCode": 200}

    bronze_key = f"{BRONZE_PREFIX}{key}"

    print(f"Movendo para: {bronze_key}")

    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=bronze_key
        )

        s3.delete_object(
            Bucket=bucket,
            Key=key
        )

        print("Arquivo movido com sucesso")

    except Exception as e:
        print("Erro ao mover arquivo:", str(e))
        raise e

    return {
        "statusCode": 200,
        "body": f"{key} → {bronze_key}"
    }
