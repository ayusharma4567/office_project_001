from dagster import MonthlyPartitionsDefinition
from ..assets import contents 

start_date = contents.START_DATE
end_date = contents.END_DATE

monthly_partition = MonthlyPartitionsDefinition(
    start_date = start_date,
    end_date = end_date
)