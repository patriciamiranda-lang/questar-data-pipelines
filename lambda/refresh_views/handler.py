import boto3
import time

REDSHIFT_WORKGROUP = "default-workgroup"
DATABASE_NAME = "dev"

redshift = boto3.client(
    "redshift-data",
    region_name="us-east-2"
)

def lambda_handler(event, context):
    response = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=DATABASE_NAME,
        Sql="CALL admin.refresh_all_mvs();"
    )

    statement_id = response["Id"]

    # Espera a procedure finalizar
    while True:
        desc = redshift.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status in ["FINISHED"]:
            return {
                "status": "SUCCESS",
                "statement_id": statement_id
            }

        if status in ["FAILED", "ABORTED"]:
            raise Exception(f"Redshift failed: {desc.get('Error')}")

        time.sleep(5)
