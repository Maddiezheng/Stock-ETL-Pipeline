from api_utils.api_factory import APIHandler
from api_utils.get_api_data import normalize_events_to_csv
from prefect import flow, task
from typing import Optional

DEFAULT_PARAMS = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min'
}

@task
def get_stock_data(stock_params: dict):
    # Initialize handler
    handler = APIHandler()
    # Get data from API
    data = handler.get_trading_data(stock_params)
    return data

@task
def save_to_csv(data: dict):
    normalize_events_to_csv(data)


@flow
def ingest_api_data_to_csv(stock_parmas: Optional[dict]=DEFAULT_PARAMS):
    data = get_stock_data(stock_parmas)
    save_to_csv(data)


if __name__ == "__main__":
    ingest_api_data_to_csv.serve(
        name='prefect-api-to-csv-deployment', cron='0 0 * * *'
    )