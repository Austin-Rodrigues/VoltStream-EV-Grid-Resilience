from utils.logging_utils import logger
from pyspark.sql import DataFrame 
from pyspark.sql.utils import AnalysisException
from databricks.sdk.runtime import spark

def write_delta_table(data: DataFrame, table_path: str, mode: str):
    try:
        logger.info(f"Loading data into {table_path} using mode {mode}")
        record_count = data.count()
        data.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)        
        logger.info(f"Successfully loaded {record_count} records into {table_path} using mode {mode}")

    except AnalysisException as analysis_err:
        logger.error(f" Analysis error occured {analysis_err}")
        raise
    except Exception as err:
        logger.error(f" Error occured {err}")
        raise

def read_delta_table(table_path: str):
    try:
        logger.info(f"Reading data from {table_path}")
        data = spark.read.format("delta").table(table_path)
        logger.info(f"Successfully read table {table_path}")
        return data
    except AnalysisException as analysis_err:
        logger.error(f" Analysis error occured {analysis_err}")
        return None
    except Exception as err:
        logger.error(f" Error occured {err}")
        return None