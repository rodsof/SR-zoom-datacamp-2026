# 🚕 NYC Taxi Analytics with dbt & DuckDB

## Homework Submission

------------------------------------------------------------------------

## Q1. What does `dbt run --select int_trips_unioned` build?

When executing:

``` bash
dbt run --select int_trips_unioned
```

dbt builds:

-   The model `int_trips_unioned`
-   All of its upstream dependencies
-   It does **not** build downstream models unless explicitly selected

------------------------------------------------------------------------

## Q2. A new value `6` appears in `payment_type`. What happens during `dbt test`?

When running:

``` bash
dbt test
```

The test **fails**.

Reason:

-   The project includes an accepted values test for `payment_type`
-   The new value `6` is not included in the accepted values list
-   dbt flags this as a test failure

------------------------------------------------------------------------

## Q3. Count of records in `fct_monthly_zone_revenue`

``` sql
SELECT COUNT(1)
FROM fct_monthly_zone_revenue;
```

This returns the total number of aggregated monthly zone-level revenue
records.

------------------------------------------------------------------------

## Q4. Zone with the highest revenue for Green taxis in 2020

``` sql
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND EXTRACT(year FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

### Result

  pickup_zone         total_revenue
  ------------------- ---------------
  East Harlem North   1,817,037.350

**Conclusion:**\
The zone with the highest total revenue for Green taxis in 2020 is
**East Harlem North**.

------------------------------------------------------------------------

## Q5. Total Green taxi trips in October 2019

``` sql
SELECT COUNT(1) AS total_trips
FROM fct_trips
WHERE service_type = 'Green'
  AND pickup_datetime >= DATE '2019-10-01'
  AND pickup_datetime <  DATE '2019-11-01';
```

### Result

  total_trips
  -------------
  384,624

**Conclusion:**\
There were **384,624 Green taxi trips** in October 2019.

------------------------------------------------------------------------

## Q6. FHV Trips -- Filtering `dispatching_base_num IS NULL`

### Step 1 --- Load FHV Data

The script downloads 2019 FHV CSV files, converts them to Parquet, and
loads them into DuckDB under schema `prod` as:

    prod.fhv_tripdata_2019

------------------------------------------------------------------------

### Step 2 --- Staging Model

``` sql
with source as (
    select * from {{ source('raw', 'fhv_tripdata_2019') }}
)

select
    dispatching_base_num,
    CAST(pickup_datetime AS timestamp)      as pickup_datetime,
    CAST(dropoff_datetime AS timestamp)     as dropoff_datetime,
    PUlocationID                             as pickup_location_id,
    DOlocationID                             as dropoff_location_id,
    SR_Flag                                  as sr_flag,
    CAST(Affiliated_base_number AS string)   as affiliated_base_num
from source
where dispatching_base_num is not null
```

------------------------------------------------------------------------

### Step 3 --- Run the Model

``` bash
uv run dbt run --select stg_fhv_tripdata
```

------------------------------------------------------------------------

### Step 4 --- Validation Query

``` sql
SELECT COUNT(1) AS cnt
FROM stg_fhv_tripdata
WHERE dispatching_base_num IS NULL;
```

### Result

    43,244,693


**Conclusion:**\
All rows with `dispatching_base_num IS NULL` are successfully filtered
out in the staging model.

------------------------------------------------------------------------

# ✅ Summary

This homework demonstrates:

-   dbt model selection behavior
-   Schema testing with accepted values
-   Revenue aggregation analysis
-   Time-based filtering
-   Data ingestion and transformation with DuckDB
-   Clean staging logic with validation
