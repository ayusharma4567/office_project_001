from dagster import EnvVar
from dagster_snowflake_pandas import SnowflakePandasIOManager

snowflake_io_manager = SnowflakePandasIOManager(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
)
