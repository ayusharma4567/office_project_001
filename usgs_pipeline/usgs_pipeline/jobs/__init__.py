from dagster import define_asset_job,AssetSelection
from ..partitions import monthly_partition

extraction_job = define_asset_job(
    name="extraction_job",
    partitions_def=monthly_partition,
    selection = AssetSelection.groups("extraction_group")
)
