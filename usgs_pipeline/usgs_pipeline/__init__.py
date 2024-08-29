from dagster import Definitions, load_assets_from_modules
from .assets import extraction_stage
from .resources import snowflake_io_manager
from .jobs import extraction_job
from .schedules import extraction_schedule

# Load all assets from the assets module
extraction_assets = load_assets_from_modules([extraction_stage])
all_jobs = [extraction_job]
all_schedule = [extraction_schedule]

# Define the Definitions object
defs = Definitions(
    assets=[*extraction_assets],  # Unpack the list directly into the assets parameter
    resources={
        "snowflake_io_manager": snowflake_io_manager
    },
    jobs = all_jobs,
    schedules = all_schedule,

)
