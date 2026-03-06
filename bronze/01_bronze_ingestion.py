from utils.logging_utils import logger
from utils.delta_utils import write_delta_table
from utils.api_utils import fetch_ocm_stations
from utils.constants import CATALOG_NAME, BRONZE_SCHEMA
from pyspark.sql.functions import current_timestamp, lit, to_json, struct
from databricks.sdk.runtime import dbutils

dbutils.widgets.text("country_code_input", "US", "Country Code")
country_code = dbutils.widgets.get("country_code_input")

def data_ingestion(dbutils):
    logger.info(f"Data Ingestion initialized for {country_code}")
    data = fetch_ocm_stations(country_code, dbutils)
    if not data:
        logger.warning(f"No data returned for {country_code}. Skipping write")
        return
    df = spark.createDataFrame(data)

    stations = df.select(
        current_timestamp().alias("ingested_at"),
        lit("ocm").alias("source"),
        to_json(struct("*")).alias("raw_payload")
    )
    
    table_path = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.stations"
    write_delta_table(stations, table_path, "append")
    logger.info(f"Data Ingestion of {stations.count()} stations into Delta Table completed for country code = {country_code}")

data_ingestion(dbutils)