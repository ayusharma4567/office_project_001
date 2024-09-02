from dagster import EnvVar
from dagster_snowflake_pandas import SnowflakePandasIOManager
from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource
from ..project import dbt_project

snowflake_io_manager = SnowflakePandasIOManager(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
)

snowflake_database = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema="final_stage",
)

dbt_resource =  DbtCliResource(
    project_dir=dbt_project,
)