from dagster import asset, AssetExecutionContext,MaterializeResult
from dagster_snowflake import SnowflakeResource
@asset(
        deps=["Earthquake_Facts","Location_Dimension"]
)
def frequent_earthquake_location(snowflake:SnowflakeResource) -> MaterializeResult:
    """
    Creating asset with dbt_asset dependences to store in snowflake final_stage schema
    """
    target_schema = "final_stage"
    target_table = "frequent_earthquake_location"

    query = f"""
        select 
            ld.place as location,
            count(ef.earthquake_id) as earthquake_count
        from
            Earthquake_Facts ef
        join
            Location_Dimension ld on ef.location_id = ld.location_id
        group by
            ld.place
        order by
            earthquake_count desc
        limit 10;
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        create_table_query = f"""
        create table if not exist {target_schema}.{target_table}(
        location string,
        earthquake_count integer
        );
        """
        cursor.execute(create_table_query)

        insert_query = f"""
        insert into {target_schema}.{target_table}(location,earthquake_count)
        values (%s, %s);
        """
        cursor.executemany(insert_query,result)

        return MaterializeResult(
            metadata={"rows_inserted":len(result)},
            ouput = f"Data inserted into {target_schema}.{target_table}"
        )



