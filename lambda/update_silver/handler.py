import boto3
import time
import logging
from datetime import datetime

# ===============================================================
# LOGGING
# ===============================================================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ===============================================================
# CONFIGURAÇÕES
# ===============================================================
REDSHIFT_WORKGROUP = "default-workgroup"
DATABASE_NAME = "dev"
DW_SCHEMA = "dw_redshift"

# ===============================================================
# CLIENTE REDSHIFT DATA API
# ===============================================================
redshift = boto3.client(
    "redshift-data",
    region_name="us-east-2"
)

# ===============================================================
# CLIENTE LAMBDA (PARA DISPARAR REFRESH DE MVs)
# ===============================================================
lambda_client = boto3.client(
    "lambda",
    region_name="us-east-2"
)

# ===============================================================
# FUNÇÃO PARA EXECUTAR SQL COM POLLING
# ===============================================================
def execute_sql(sql: str):
    logger.info(f"Executando SQL:\n{sql}")

    response = redshift.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=DATABASE_NAME,
        Sql=sql
    )

    statement_id = response["Id"]

    while True:
        desc = redshift.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status in [
            "FINISHED",
            "FINISHED_WITH_WARNINGS",
            "FINISHED_WITH_ERRORS"
        ]:
            break

        if status in ["FAILED", "ABORTED"]:
            raise Exception(
                f"SQL FAILED\n"
                f"Status: {status}\n"
                f"Error: {desc.get('Error')}\n"
                f"Message: {desc.get('ErrorMessage')}"
            )

        time.sleep(2)

    if desc.get("ErrorMessage"):
        logger.warning(f"SQL executado com avisos: {desc.get('ErrorMessage')}")

    return desc
# ===============================================================
# SQL DE CARGA (SILVER → DW)
# ===============================================================
LOAD_SQL = {

    # =======================
    # DIM: DRIVERS
    # =======================
    "d_drivers": """
        INSERT INTO dw_redshift.d_drivers (
            driver_id,
            driver_name,
            group_id,
            driver_code,
            worker_id,
            p_source_date
        )
        SELECT
            CAST(driver_id AS bigint),
            CAST(driver_name AS varchar),
            CAST(group_id AS bigint),
            CAST(driver_code AS varchar),
            CAST(worker_id AS bigint),
            CAST(p_source_date AS varchar)
        FROM dw.drivers
        WHERE driver_id IS NOT NULL;
    """,

    # =======================
    # DIM: GROUPS
    # =======================
    "d_groups": """
        INSERT INTO dw_redshift.d_groups (
            group_id,
            group_name,
            parent_group_id,
            parent_group_name,
            client_id,
            p_source_date,
            external_id
        )
        SELECT
            CAST(group_id AS bigint),
            CAST(group_name AS varchar),
            CAST(parent_group_id AS bigint),
            CAST(parent_group_name AS varchar),
            CAST(client_id AS bigint),
            CAST(p_source_date AS varchar),
            CAST(external_id AS varchar)
        FROM dw.groups
        WHERE group_id IS NOT NULL;
    """,

    # =======================
    # DIM: SCHEMES
    # =======================
    "d_schemes": """
        INSERT INTO dw_redshift.d_schemes (
            scheme_id,
            scheme_description,
            description,
            parent_scheme,
            scheme_type,
            p_load_date,
            p_source_date
        )
        SELECT
            CAST(scheme_id AS bigint),
            CAST(scheme_description AS varchar(600)),
            CAST(description AS varchar(600)),
            CAST(parent_scheme AS varchar(300)),
            CAST(scheme_type AS varchar(200)),
            TO_CHAR(current_date, 'YYYY-MM-DD'),
            CAST(p_source_date AS date)
        FROM dw.schemes
        WHERE scheme_id IS NOT NULL;
    """,

    # =======================
    # DIM: VEHICLES
    # =======================
    "d_vehicles": """
        INSERT INTO dw_redshift.d_vehicles (
            vehicle_id,
            license_number,
            group_id,
            manufacturer,
            model,
            unit_serial_number,
            vin,
            vehicle_engine_type,
            body_configuration,
            p_source_date
        )
        SELECT
            CAST(vehicle_id AS bigint),
            CAST(license_number AS varchar),
            CAST(group_id AS bigint),
            CAST(manufacturer AS varchar),
            CAST(model AS varchar),
            CAST(unit_serial_number AS bigint),
            CAST(vin AS varchar),
            CAST(vehicle_engine_type AS varchar),
            CAST(body_configuration AS varchar),
            CAST(p_source_date AS varchar)
        FROM dw.vehicles
        WHERE vehicle_id IS NOT NULL;
    """,

    # =======================
    # FACT: EVENTS
    # =======================
    "t_events": """
        INSERT INTO dw_redshift.t_events (
    event_id,
    vehicle_id,
    scheme_id,
    drive_id,
    start_time,
    start_location,
    start_latitude,
    start_longitude,
    direction,
    start_mileage,
    end_time,
    end_location,
    end_latitude,
    end_longitude,
    end_mileage,
    speed,
    max_speed,
    p_source_date,
    p_load_date
)
SELECT
    CAST(event_id AS bigint),
    CAST(vehicle_id AS bigint),
    CAST(scheme_id AS bigint),
    CAST(drive_id AS bigint),
    start_time,
    CAST(start_location AS varchar),
    CAST(start_latitude AS double precision),
    CAST(start_longitude AS double precision),
    CAST(direction AS double precision),
    CAST(start_mileage AS double precision),
    end_time,
    CAST(end_location AS varchar),
    CAST(end_latitude AS double precision),
    CAST(end_longitude AS double precision),
    CAST(end_mileage AS double precision),
    CAST(speed AS double precision),
    CAST(max_speed AS double precision),
    CAST(p_source_date AS date),
    CURRENT_DATE               
FROM dw.events
WHERE event_id IS NOT NULL;

    """,

    # =======================
    # FACT: TRIPS
    # =======================
    "t_trips": """
        INSERT INTO dw_redshift.t_trips (
            drive_id,
            vehicle_id,
            driver_id,
            start_drive,
            start_location,
            start_latitude,
            start_longitude,
            end_drive,
            end_location,
            end_latitude,
            end_longitude,
            drive_duration,
            idle_duration,
            mileage,
            avg_speed,
            turn1,
            turn2,
            turn3,
            break1,
            break2,
            break3,
            acceleration1,
            acceleration2,
            acceleration3,
            speed_road1,
            speed_road2,
            speed_road3,
            energy_consumption,
            breaking_system,
            gearbox,
            engine,
            clutch_system,
            start_energy_level,
            end_energy_level,
            energy_used,
            start_soc,
            end_soc,
            p_source_date
        )
        SELECT
            CAST(drive_id AS bigint),
            CAST(vehicle_id AS bigint),
            CAST(driver_id AS bigint),
            start_drive,
            CAST(start_location AS varchar),
            CAST(start_latitude AS double precision),
            CAST(start_longitude AS double precision),
            end_drive,
            CAST(end_location AS varchar),
            CAST(end_latitude AS double precision),
            CAST(end_longitude AS double precision),
            CAST(drive_duration AS double precision),
            CAST(idle_duration AS double precision),
            CAST(mileage AS double precision),
            CAST(avg_speed AS double precision),
            CAST(turn1 AS integer),
            CAST(turn2 AS integer),
            CAST(turn3 AS integer),
            CAST(break1 AS integer),
            CAST(break2 AS integer),
            CAST(break3 AS integer),
            CAST(acceleration1 AS integer),
            CAST(acceleration2 AS integer),
            CAST(acceleration3 AS integer),
            CAST(speed_road1 AS integer),
            CAST(speed_road2 AS integer),
            CAST(speed_road3 AS integer),
            CAST(energy_consumption AS integer),
            CAST(breaking_system AS integer),
            CAST(gearbox AS integer),
            CAST(engine AS integer),
            CAST(clutch_system AS integer),
            CAST(start_energy_level AS double precision),
            CAST(end_energy_level AS double precision),
            CAST(energy_used AS double precision),
            CAST(start_soc AS double precision),
            CAST(end_soc AS double precision),
            CAST(p_source_date AS varchar)
        FROM dw.trips;
    """
}

# ===============================================================
# HANDLER
# ===============================================================
def lambda_handler(event, context):

    run_id = context.aws_request_id

    logger.info("========================================")
    logger.info("Lambda lambda-load-dw-prod EXECUTADO")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Horário UTC: {datetime.utcnow().isoformat()}")
    logger.info("Evento recebido:")
    logger.info(event)
    logger.info("========================================")

    # ===============================================================
    # CARGA DAS TABELAS DW
    # ===============================================================
    for table, sql in LOAD_SQL.items():
        logger.info(f"Iniciando carga da tabela {DW_SCHEMA}.{table}")

        execute_sql(f"DELETE FROM {DW_SCHEMA}.{table};")
        execute_sql(sql)

        logger.info(f"Tabela {table} carregada com sucesso")

    # ===============================================================
    # DISPARAR REFRESH DAS MATERIALIZED VIEWS
    # ===============================================================
    logger.info("Invocando lambda-refresh-mv-prod")

    lambda_client.invoke(
        FunctionName="lambda-refresh-mv-prod",
        InvocationType="Event"  # assíncrono
    )

    logger.info("lambda-refresh-mv-prod invocada com sucesso")

    return {
        "status": "SUCCESS",
        "run_id": run_id,
        "tables_updated": list(LOAD_SQL.keys()),
        "mv_refresh": "TRIGGERED"
    }
