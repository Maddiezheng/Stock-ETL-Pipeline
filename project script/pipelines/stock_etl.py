from prefect import flow, task
from etl_utils import ingest_data_to_delta, setup_spark_session, read_data_from_delta, deduplication,run_data_validations
from dq_utils import spark_scan
from transform import stock_events_normalizer
from api_utils.api_factory import APIHandler
from typing import Optional
from datetime import datetime, timedelta
from transform import transform_daily_trades


COMPANY_PARAMS = [
    {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': 'NVDA',
        'interval': '1min',
        'outputsize': 'full',
    },
    {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': 'TSLA',
        'interval': '1min',
        'outputsize': 'full',
    },
    {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': 'IBM',
        'interval': '1min',
        'outputsize': 'full',
    },
    {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': 'AAPL',
        'interval': '1min',
        'outputsize': 'full',
    },
]

@task
def setup_spark():
    return setup_spark_session()


@task
def extract_api_data(params):
    """
    Extract stock data from API
    """
    handler = APIHandler()
    return handler.get_trading_data(params)


@task
def transform_bronze(data, spark):
    """
    Transform API data to bronze format
    """
    df = stock_events_normalizer(data, spark)
    return df

@task
def bronze_dq_check(df, spark, soda_check_name):
    spark_scan(df, spark, soda_check_name)


@task
def load_to_bronze(df):
    """
    Load data to bronze layer
    """
    ingest_data_to_delta(df, "trade")

@task
def extract_bronze_data(spark, date: Optional[str] = None, symbol: Optional[str] = None):
    """
    Extract data from Bronze Layer

    Args:
        spark: SparkSession
        date: Optional date to filter data (format: 'YYYY-MM-DD')
        symbol: Optional stock symbol to filter data (e.g., 'TSLA', 'AAPL')
    Returns:
        DataFrame
    """

    # Create filter conditions
    filter_conditions = []

    if date:
        filter_conditions.append(f"partition = '{date}'")
    
    if symbol:
        filter_conditions.append(f"symbol = '{symbol}'")

    # Combine filter conditions
    if filter_conditions:
        filter_condition = " AND ".join(filter_conditions)
        df = read_data_from_delta("trade", spark, filter_condition)
    else:
        df = read_data_from_delta("trade", spark)

    return df


@task
def transform_to_daily(df):
    """
    Transform data to daily aggregates
    """

    daily_df = transform_daily_trades(df)
    return daily_df

@task 
def silver_dq_check(df, spark, soda_check_name):
    spark_scan(df, spark, soda_check_name)

@task
def load_to_silver(df):
    """
    Load data silver layer
    """
    ingest_data_to_delta(
        df,
        table_name = "daily_trades",
        mode = "append",
        storage_path = "s3a://stocklake/delta"
    )

@flow
def silver_etl_pipeline(date: Optional[str] = None, symbol: Optional[str] = None):
    """
    End-to-end ETL pipeline from API to Silver

    Args:
        date: 
        Optional date to process in format 'YYYY-MM-DD'
        If none, process all data
        symbol: Optional stock symbol to process

    """
    # Step 1: Set up a Spark Session
    spark = setup_spark()

    # Step 2: Bronze Layer ETL
    for params in COMPANY_PARAMS:
        api_data = extract_api_data(params)
        bronze_df = transform_bronze(api_data, spark)
        bronze_dq_check(bronze_df, spark, "trade")
        load_to_bronze(bronze_df)

    # Step 3 : Silver Layer ETL 
    bronze_df = extract_bronze_data(spark, date, symbol)  # Extract (Read data from the Bronze layer)
    daily_df = transform_to_daily(bronze_df)    # Transform (Transform to daily data)
    silver_dq_check(daily_df, spark, "daily_trades")
    load_to_silver(daily_df)                        # Load
    



# # Test
# if __name__ == "__main__":
#     silver_etl_pipeline(
#         date='2024-12-16',
#         symbol='AAPL'
#     )

# Deploy
if __name__ == "__main__":
    # Deploy with daily schedule
    silver_etl_pipeline()

# # Deploy
# if __name__ == "__main__":
#     # Deploy with daily schedule
#     silver_etl_pipeline.serve(
#         name='silver-layer-daily-etl',
#         cron='0 1 * * *' 
#     )
