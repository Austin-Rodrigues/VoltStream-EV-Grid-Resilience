import requests
from utils.logging_utils import logger
from utils.constants import OCM_BASE_URL, MAX_RESULTS
import time

def fetch_ocm_stations(country_code, dbutils):
  all_stations = []
  ocm_api_key = dbutils.secrets.get(scope="VoltStream", key="ocm_api_key")
  offset = 0
  param = {'offset': offset, 'maxresults': MAX_RESULTS}
  logger.info(f"Starting API call for {country_code}")
  while True:
    try:
      response = requests.get(f'{OCM_BASE_URL}/poi/?output=json&countrycode={country_code}&key={ocm_api_key}', params = param, timeout = 15)
      
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

      logger.info(f"Fetched {len(all_stations)} records for offset {offset}")
      offset += MAX_RESULTS
      param['offset'] = offset
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

  