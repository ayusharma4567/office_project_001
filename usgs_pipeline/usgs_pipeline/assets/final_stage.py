from dagster import asset, AssetExecutionContext, MaterializeResult
from dagster_snowflake import SnowflakeResource

@asset(
    deps=["Earthquake_Facts", "Location_Dimension"],
    group_name="final_query_group"
)
def frequent_earthquake_location(snowflake: SnowflakeResource) -> MaterializeResult:
    """
    Creating asset with dbt_asset dependencies to store in Snowflake final_stage schema
    """
    target_schema = "final_stage"
    target_table = "frequent_earthquake_location"

    query = f"""
        SELECT 
            ld.place AS location,
            COUNT(ef.earthquake_id) AS earthquake_count
        FROM
            consumption_stage.Earthquake_Facts ef
        JOIN
            consumption_stage.Location_Dimension ld ON ef.location_id = ld.location_id
        GROUP BY
            ld.place
        ORDER BY
            earthquake_count DESC
        LIMIT 10;
    """
    
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
            location STRING,
            earthquake_count INTEGER
        );
        """
        cursor.execute(create_table_query)

        insert_query = f"""
        INSERT INTO {target_schema}.{target_table} (location, earthquake_count)
        VALUES (%s, %s);
        """
        cursor.executemany(insert_query, result)

        return MaterializeResult(
            metadata={"rows_inserted": len(result)}
        )




