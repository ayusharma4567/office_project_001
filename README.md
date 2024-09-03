# office_project_001
office project on dagster - dbt - snowflake

## Creating a database and warehouse in Snowflake

```sql
create or replace database Demo_db;
create or replace schema Demo_db.landing_stage;
create or replace schema Demo_db.curated_stage;
create or replace schema Demo_db.consumption_stage;
create or replace schema Demo_db.final_stage;
create or replace warehouse Demo_wh with warehouse_size='large';
select * from landing_stage.extraction_stage;
select * from consumption_stage.location_dimension;
select * from consumption_stage.magnitude_type_dimension;
select * from consumption_stage.time_dimension;
select * from consumption_stage.earthquake_facts;
select * from final_stage.frequent_earthquake_location;
```

## Creating a Star Schema Model from Earthquake Data

Creating a star schema model from the provided earthquake data involves organizing the data into fact and dimension tables, which can help in efficient querying and analysis. In a star schema, the fact table contains the quantitative data (e.g., earthquake measurements), while the dimension tables provide context (e.g., location details, time details). Below is the schema based on the columns in the image.

### 1. Fact Table: `Earthquake_Facts`

This table contains the quantitative measurements and references keys from the dimension tables. Each record represents an individual earthquake event.

| Column Name        | Description                                        |
|--------------------|----------------------------------------------------|
| `Earthquake_ID`    | Unique identifier for each event                   |
| `Time_ID`          | Foreign key to Time dimension                      |
| `Location_ID`      | Foreign key to Location dimension                  |
| `Depth`            | Depth of the earthquake                            |
| `Magnitude`        | Magnitude of the earthquake                        |
| `Magnitude_Type`   | Type of magnitude measurement                      |
| `NST`              | Number of seismic stations                         |
| `GAP`              | Azimuthal gap                                      |
| `DMIN`             | Minimum distance to seismometer                    |
| `RMS`              | Root mean square of amplitude                      |
| `Horizontal_Error` | Horizontal error in location                       |
| `Depth_Error`      | Error in depth measurement                         |
| `Magnitude_Error`  | Error in magnitude measurement                     |
| `Magnitude_ST`     | Number of stations contributing to magnitude       |
| `Status`           | Status of the earthquake data                      |
| `Location_Source`  | Source of location data                            |
| `Magnitude_Source` | Source of magnitude data                           |

### 2. Dimension Table: `Time_Dimension`

This table includes detailed time information for easy querying by date or time.

| Column Name | Description                          |
|-------------|--------------------------------------|
| `Time_ID`   | Primary key (Unique ID for time)     |
| `Date`      | Date of the event                    |
| `Time`      | Time of the event                    |
| `Year`      | Year of the event                    |
| `Month`     | Month of the event                   |
| `Day`       | Day of the event                     |
| `Hour`      | Hour of the event                    |
| `Minute`    | Minute of the event                  |
| `Second`    | Second of the event                  |

### 3. Dimension Table: `Location_Dimension`

This table includes detailed information about the location of the earthquake.

| Column Name  | Description                       |
|--------------|-----------------------------------|
| `Location_ID`| Primary key (Unique ID for location) |
| `Latitude`   | Latitude of the event             |
| `Longitude`  | Longitude of the event            |
| `Place`      | Descriptive place name            |
| `Region`     | Region or area classification     |

### 4. Dimension Table: `Magnitude_Type_Dimension`

This table includes information about the type of magnitude measurement used.

| Column Name         | Description                             |
|---------------------|-----------------------------------------|
| `Magnitude_Type_ID` | Primary key (Unique ID for magnitude type) |
| `Magnitude_Type`    | Type of magnitude (e.g., mb, ml, md)    |

## Implementation Steps

### 1. Create the Dimension Tables:

- Populate `Time_Dimension` with distinct dates and times from your data, along with additional time breakdowns (year, month, day, hour, etc.).
- Populate `Location_Dimension` with distinct latitude, longitude, and place combinations.
- Populate `Magnitude_Type_Dimension` with distinct magnitude types.

### 2. Create the Fact Table:

- Populate `Earthquake_Facts` using foreign keys from the dimension tables. For each earthquake record:
  - Assign a `Time_ID` by matching the event time with `Time_Dimension`.
  - Assign a `Location_ID` by matching latitude, longitude, and place with `Location_Dimension`.
  - Assign a `Magnitude_Type_ID` by matching the magnitude type.

