from dagster import ScheduleDefinition
from ..jobs import extraction_job

extraction_schedule = ScheduleDefinition(
    job =extraction_job,
    cron_schedule = "* * * * *"
)