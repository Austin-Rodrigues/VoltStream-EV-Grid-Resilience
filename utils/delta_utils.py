from utils.logging_utils import logger
from pyspark.sql import DataFrame 
from pyspark.sql.utils import AnalysisException

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
    except Exception as err:
        logger.error(f" Error occured {err}")