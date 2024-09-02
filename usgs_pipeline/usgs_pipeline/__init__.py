from dagster import Definitions, load_assets_from_modules
from .assets import extraction_stage,dbt,final_stage
from .resources import snowflake_io_manager,dbt_resource,snowflake_database
from .jobs import extraction_job
from .schedules import extraction_schedule

extraction_assets = load_assets_from_modules([extraction_stage])
dbt_analytics_assets = load_assets_from_modules(modules=[dbt])
final_assets = load_assets_from_modules([final_stage])

all_jobs = [extraction_job]
all_schedule = [extraction_schedule]

# Define the Definitions object
defs = Definitions(
    assets=[*extraction_assets, *dbt_analytics_assets, *final_assets],  # Unpack the list directly into the assets parameter
    resources={
        "snowflake_io_manager": snowflake_io_manager,
        "snowflake":snowflake_database,
        "dbt" : dbt_resource,
    },
    jobs = all_jobs,
    schedules = all_schedule,

)
