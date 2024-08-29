from setuptools import find_packages, setup

setup(
    name="usgs_pipeline",
    packages=find_packages(exclude=["usgs_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-snowflake",
        "pandas",
        "dagster_dbt",
        "dbt-snowflake",
        "dagster_snowflake_pandas",
        "dagster-cloud",
        "dbt_core",
        "dbt-snowflake",
        "snowflake-connector-python"
        
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
