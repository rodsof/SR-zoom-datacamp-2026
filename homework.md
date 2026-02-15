Q1: dbt run --select int_trips_unioned builds which models?

Q2: New value 6 appears in payment_type. What happens on dbt test?
It fails. All values are unique.

Q3: Count of records in fct_monthly_zone_revenue?
SELECT COUNT(1) FROM fct_monthly_zone_revenue

Q4: Zone with highest revenue for Green taxis in 2020?
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND EXTRACT(year FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;


Result:
 pickup_zone    │ total_revenue │
│      varchar      │ decimal(38,3) │
├───────────────────┼───────────────┤
│ East Harlem North │  1817037.350 


Q5: Total trips for Green taxis in October 2019?
SELECT COUNT(1) AS total_trips
  FROM fct_trips
  WHERE service_type = 'Green'
    AND pickup_datetime >= DATE '2019-10-01'
    AND pickup_datetime <  DATE '2019-11-01';
┌─────────────┐
│ total_trips │
│    int64    │
├─────────────┤
│   384624    │
└─────────────┘

Q6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)
load_data_fhv.py:

import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def download_and_convert_files(taxi_type):
    data_dir = Path("data") / taxi_type
    data_dir.mkdir(exist_ok=True, parents=True)

    for year in [2019]:
        for month in range(1, 13):
            parquet_filename = f"fhv_tripdata_{year}-{month:02d}.parquet"
            parquet_filepath = data_dir / parquet_filename

            if parquet_filepath.exists():
                print(f"Skipping {parquet_filename} (already exists)")
                continue

            # Download CSV.gz file
            csv_gz_filename = f"fhv_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir / csv_gz_filename

            response = requests.get(
                f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream=True
            )
            response.raise_for_status()

            with open(csv_gz_filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"Converting {csv_gz_filename} to Parquet...")
            con = duckdb.connect()
            con.execute(
                f"""
                # Homework

                ## Q1: dbt run --select int_trips_unioned builds which models?

                (Answer: builds the upstream staging models such as `stg_green_tripdata` and `stg_yellow_tripdata`, then `int_trips_unioned`)

                ---

                ## Q2: New value 6 appears in `payment_type`. What happens on `dbt test`?

                > It fails. The `accepted_values` (lookup) test for `payment_type` reports the unexpected value and dbt exits with a non-zero status.

                ---

                ## Q3: Count of records in `fct_monthly_zone_revenue`?

                ```sql
                SELECT COUNT(1) FROM fct_monthly_zone_revenue;
                ```

                ---

                ## Q4: Zone with highest revenue for Green taxis in 2020?

                ```sql
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

                Example result:

                ```
                pickup_zone       | total_revenue
                ------------------+---------------
                East Harlem North | 1817037.350
                ```

                ---

                ## Q5: Total trips for Green taxis in October 2019?

                ```sql
                SELECT COUNT(1) AS total_trips
                FROM fct_trips
                WHERE service_type = 'Green'
                    AND pickup_datetime >= DATE '2019-10-01'
                    AND pickup_datetime <  DATE '2019-11-01';
                ```

                Example result:

                ```
                total_trips
                -----------
                384624
                ```

                ---

                ## Q6: Count of records in `stg_fhv_tripdata` (filter `dispatching_base_num IS NULL`)

                Ingestion script (converts CSV.gz → Parquet and loads into `prod.fhv_tripdata_2019`): `load_data_fhv.py`.

                Staging model `models/staging/stg_fhv_tripdata.sql` (filter + rename):

                ```sql
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
                where dispatching_base_num is not null;
                ```

                Build and count NULLs (should be zero after filtering):

                ```bash
                uv run dbt run --select stg_fhv_tripdata
                duckdb taxi_rides_ny.duckdb -c "SELECT COUNT(*) AS cnt FROM stg_fhv_tripdata WHERE dispatching_base_num IS NULL;"
                ```

                ---

                If you need the `source` to point to schema `prod`, update `models/staging/sources.yml` to set `schema: prod` and ensure `~/.dbt/profiles.yml` uses `schema: prod` for your DuckDB output, then re-run `dbt debug`.
