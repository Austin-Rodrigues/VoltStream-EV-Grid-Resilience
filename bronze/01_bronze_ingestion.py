from datetime import datetime
from utils.logging_utils import logger
from utils.delta_utils import write_delta_table
from utils.api_utils import fetch_ocm_stations
from utils.constants import CATALOG_NAME, BRONZE_SCHEMA
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit, to_json, struct, col
from databricks.sdk.runtime import dbutils
from databricks.sdk.runtime import spark
import json

dbutils.widgets.text("country_code_input", "US", "Country Code")
country_code = dbutils.widgets.get("country_code_input")

def data_ingestion(dbutils):
    try:
        start_time = datetime.now()
        metadata_table_path = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.metadata"
        logger.info(f"Data Ingestion initialized for {country_code}")
        data = fetch_ocm_stations(country_code, dbutils, metadata_table_path)
        if not data:
            logger.warning(f"No data returned for {country_code}. Skipping write")
            return
        json_data = [{"raw_payload": json.dumps(record)} for record in data]
        df = spark.createDataFrame(json_data)

        stations = df.select(
            current_timestamp().alias("ingested_at"),
            lit("ocm").alias("source"),
            col("raw_payload")
        )
        
        table_path = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.stations"
        write_delta_table(stations, table_path, "append")
        record_count = df.count()
        logger.info(f"Data Ingestion/Update of {record_count} stations into Delta Table completed for country code = {country_code}")

        meta_data = {"process_name": "bronze_ingestion",
                "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "last_run_timestamp": data[-1]["DateLastStatusUpdate"],
                "status": "success",
                "records_fetched": record_count
        }
        metadata_df = spark.createDataFrame([Row(**meta_data)])
        write_delta_table(metadata_df, metadata_table_path, "append")

    except Exception as e:
        logger.error(f"Data Ingestion failed for {country_code} with error: {e}")
        meta_data = {
            "process_name": "bronze_ingestion",
            "start_time": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "last_run_timestamp": data[-1]['DateLastStatusUpdate'] if data else None,
            "status": "failed",
            "records_fetched": record_count if 'record_count' in dir() else 0
        }
        metadata_df = spark.createDataFrame([Row(**meta_data)])
        write_delta_table(metadata_df, metadata_table_path, "append")
        raise e
    

data_ingestion(dbutils)