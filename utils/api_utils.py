import requests
from utils.logging_utils import logger
from utils.constants import OCM_BASE_URL, MAX_RESULTS

def fetch_ocm_stations(country_code):
  all_stations = []
  ocm_api_key = dbutils.secrets.get(scope="VoltStream", key="ocm_api_key")
  offset = 0
  param = {'offset': offset, 'maxresults': MAX_RESULTS}
  logger.info(f"Starting API call for {country_code}")
  while True:
    try:
      response = requests.get(f'{ocm_base_url}/poi/?output=json&countrycode={country_code}&key={ocm_api_key}', params = param, timeout = 15)
      response.raise_for_status()
      
      if response.json() == []:
        logger.info(f"Pagination complete. Total records: {len(all_stations)}")
        break
      page_data = response.json()
      all_stations += page_data
      logger.info(f"Fetched {len(all_stations)} records for offset {offset}")
      offset += MAX_RESULTS
      param['offset'] = offset
      
  
    except requests.exceptions.HTTPError as http_err:
      logger.error(f"HTTP error occured {http_err}")
    except requests.exceptions.ConnectionError as con_err:
      logger.error(f"Connection error occured {con_err}")
    except requests.exceptions.Timeout as timeout_err:
      logger.warning(f"Timeout error occured {timeout_err}")
    except Exception as err:
      logger.error(f"Error occured {err}")

  return all_stations

  