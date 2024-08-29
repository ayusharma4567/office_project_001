from dagster import asset, AssetExecutionContext
import requests
import pandas as pd
from io import StringIO
from ..partitions import monthly_partition

@asset(
    group_name="extraction_group",
    io_manager_key="snowflake_io_manager",
    partitions_def=monthly_partition,
    metadata={"partition_expr": "time"}  # Specify the partition column
)
def extraction_stage(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Extraction of USGS data into a Snowflake table using SnowflakePandasIOManager.
    The table name is dynamically generated based on the partition year and month.
    """
    partition_date_str = context.partition_key

    # Extract year and month using slicing
    year_month = partition_date_str[:-3]  # e.g., "2024-08"

    # Construct starttime and endtime using the extracted year and month
    starttime = f"{year_month}-01"
    endtime = (pd.to_datetime(starttime) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "csv",
        "starttime": starttime,  # Dynamic starttime based on partition
        "endtime": endtime,      # Dynamic endtime based on partition
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        # Read the CSV content from the response using StringIO
        df = pd.read_csv(StringIO(response.text), header=0)  # Adjust based on CSV format

        # Define the datetime format
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"

        # Ensure the 'time' column is of datetime type
        try:
            df['time'] = pd.to_datetime(df['time'], format=datetime_format, errors='coerce')  # Coerce errors to NaT
        except Exception as e:
            context.log.error(f"Date parsing error: {e}")

        # Construct dynamic table name based on year and month
        table_name = f"earthquake_data_{year_month.replace('-', '_')}"  # e.g., "earthquake_data_2024_08"

        # Return DataFrame with metadata for Snowflake
        return df
    else:
        context.log.error(f"Error fetching data: {response.status_code}")
        return pd.DataFrame()  # Return empty DataFrame in case of error


