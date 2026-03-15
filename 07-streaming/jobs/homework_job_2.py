from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_green_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips-october-2025',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_session_window_sink(t_env):
    table_name = 'trips_session_per_pulocation'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (session_start, session_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_longest_session_sink(t_env):
    table_name = 'longest_session_per_pulocation'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT,
            PRIMARY KEY (PULocationID, session_start, session_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def log_session_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_green_trips_source_kafka(t_env)
        session_sink = create_session_window_sink(t_env)
        longest_session_sink = create_longest_session_sink(t_env)

        # Session window aggregation
        t_env.execute_sql(f"""
        INSERT INTO {session_sink}
        SELECT
            window_start as session_start,
            window_end as session_end,
            PULocationID,
            COUNT(*) as num_trips
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID
        """).wait()

        # Find the PULocationID with the longest session (most trips in a single session)
        t_env.execute_sql(f"""
        INSERT INTO {longest_session_sink}
        SELECT
            PULocationID,
            session_start,
            session_end,
            num_trips
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (ORDER BY num_trips DESC) as rn
            FROM {session_sink}
        )
        WHERE rn = 1
        """).wait()

    except Exception as e:
        print("Session window aggregation failed:", str(e))

if __name__ == '__main__':
    log_session_aggregation()