import requests
from utils.logging_utils import logger
from utils.constants import OCM_BASE_URL, MAX_RESULTS, DEFAULT_MODIFIED_SINCE
from utils.delta_utils import read_delta_table
import time
from pyspark.sql.functions import max, col

def fetch_ocm_stations(country_code, dbutils, table_path):
  all_stations = []
  ocm_api_key = dbutils.secrets.get(scope="VoltStream", key="ocm_api_key")
  modified_since = DEFAULT_MODIFIED_SINCE

  data = read_delta_table(table_path)
  if data is None:
    param = {
      'modifiedsince': DEFAULT_MODIFIED_SINCE,
      'sortby': 'modified_asc',
      'maxresults': MAX_RESULTS,  
      'countrycode': country_code,
      'output': 'json',
      'key': ocm_api_key
    }
    logger.info(f"Starting API call for {country_code} for full load ingestion")
  else:
    last_status = data.orderBy("start_time", ascending=False).first()["status"]

    if last_status == "success":
      last_date = data.filter(col("status") == "success") \
               .orderBy("start_time", ascending=False) \
               .first()["last_run_timestamp"]
    else:
      last_date = DEFAULT_MODIFIED_SINCE

    param = {
        'modifiedsince': last_date,
        'sortby': 'modified_asc',
        'maxresults': MAX_RESULTS,  
        'countrycode': country_code,
        'output': 'json',
        'key': ocm_api_key
      }
    logger.info(f"Starting API call for {country_code} for incremental load ingestion from {last_date}")
  while True:
    try:
      response = requests.get(f'{OCM_BASE_URL}/poi/', params = param, timeout = 15)
      
      if response.status_code == 429:
        logger.warning(f"Rate limited. waiting 30 seconds before retry..")
        time.sleep(30)
        continue

      response.raise_for_status()
           
      page_data = response.json()
      all_stations += page_data
      if len(page_data) < MAX_RESULTS:
        logger.info(f"Pagination complete. Total records: {len(all_stations)}")
        break
      
      modified_since = page_data[-1]['DateLastStatusUpdate']
      param['modifiedsince'] = modified_since
      logger.info(f"Fetched {len(all_stations)} records, modifiedsince updated to {modified_since}")
      time.sleep(1)
      
  
    except requests.exceptions.HTTPError as http_err:
      logger.error(f"HTTP error occured {http_err}")
      break
    except requests.exceptions.ConnectionError as con_err:
      logger.error(f"Connection error occured {con_err}")
      break
    except requests.exceptions.Timeout as timeout_err:
      logger.warning(f"Timeout error occured {timeout_err}")
      break
    except Exception as err:
      logger.error(f"Error occured {err}")
      break

  return all_stations

  