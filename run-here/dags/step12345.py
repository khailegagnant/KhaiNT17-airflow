from __future__ import annotations
import json
import os
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import psycopg2
############################
############################
############################
############################ 
class SqlQueries:
    songplay_table_insert = """
        SELECT
                md5(events.sessionid::text || events.start_time::text) songplay_id,
                events.start_time,
                events.userid,
                events.level,
                songs.song_id,
                songs.artist_id,
                events.sessionid,
                events.location,
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """

    # user_table_insert = """
    #     SELECT distinct userid, firstname, lastname, gender, level
    #     FROM staging_events
    #     WHERE page='NextSong' AND userid IS NOT NULL
    # """

    user_table_insert = """SELECT userid, firstname, lastname, gender, level
FROM (
    SELECT 
        userid, 
        firstname, 
        lastname, 
        gender, 
        level,
        ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) as rn
    FROM staging_events
    WHERE page='NextSong' AND userid IS NOT NULL
) AS ranked_users
WHERE rn = 1"""

    song_table_insert = """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """

    artist_table_insert = """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """

    time_table_insert = """
        SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time),
               extract(month from start_time), extract(year from start_time), extract(DOW from start_time)
        FROM songplays
    """

SONG_DATA_PATH = "/opt/airflow/data/song_data"
LOG_DATA_PATH = "/opt/airflow/data/log_data"

def get_all_files(directory: str) -> list[str]:
    filepaths = []
    for root, _, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            filepaths.append(filepath)
    return filepaths

# def json_to_sql(
#     postgres_conn_id: str, table_name: str, data_path: str, columns: list[str]
# ):
#     postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
#     conn = postgres_hook.get_conn()
#     cur = conn.cursor()
#     cur.execute(f"TRUNCATE TABLE {table_name};")

#     json_files = [f for f in get_all_files(data_path) if f.endswith(".json")]
#     # print(f"Found {len(json_files)} json files to process in '{data_path}'.")

#     s_placeholders = ", ".join(["%s"] * len(columns))
#     sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({s_placeholders})"
#     rows_inserted = 0
#     for filepath in json_files:
#         with open(filepath, "r") as f:
#             try:
#                 data = json.load(f)
#                 values = [data.get(col) for col in columns]
#                 cur.execute(sql_insert, values)
#                 rows_inserted += 1
#             except json.JSONDecodeError:
#                 f.seek(0)
#                 for line in f:
#                     if line.strip():
#                         data = json.loads(line)
#                         values = [data.get(col) for col in columns]
#                         cur.execute(sql_insert, values)
#                         rows_inserted += 1
#     conn.commit()
#     print(f"Staging for table '{table_name}' complete. Inserted {rows_inserted} rows.")
#     cur.close()
#     conn.close()



def json_to_sql(
    postgres_conn_id: str, table_name: str, data_path: str, columns: list[str]
):
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name};")

    json_files = [f for f in get_all_files(data_path) if f.endswith(".json")]
    print(f"Found {len(json_files)} json files to process in '{data_path}'.")

    s_placeholders = ", ".join(["%s"] * len(columns))
    sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({s_placeholders})"
    rows_inserted = 0

    for filepath in json_files:
        with open(filepath, "r") as f:
            lines = f.readlines()  

        for line in lines:
            if not line.strip():
                continue  

            try:
                data = json.loads(line)
                
                #   to do : FIXE FORM HERE

                raw_values = [data.get(col) for col in columns]             
                cleaned_values = [None if v == "" else v for v in raw_values]
                # END OF FIX
                
                cur.execute(sql_insert, cleaned_values) 
                rows_inserted += 1

            except (json.JSONDecodeError, psycopg2.Error) as e:
                print(f"Skipping row due to error: {e}. Data: {line.strip()}")
                conn.rollback() # Rollback the failed transaction for this row
                continue

    conn.commit() 
    print(f"Staging for table '{table_name}' complete. Inserted {rows_inserted} rows.")
    cur.close()
    conn.close()




def load_table(postgres_conn_id: str, target_table: str, sql_statement: str, append_only: bool = False):

    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    print(f"Executing transformation for table: {target_table}")

    if not append_only:
        sql_formatted = f"TRUNCATE TABLE {target_table}; INSERT INTO {target_table} {sql_statement}"
    else:
        sql_formatted = f"INSERT INTO {target_table} {sql_statement}"

    postgres_hook.run(sql_formatted)
    print(f"Successfully loaded data into {target_table}.")


@dag(
    dag_id="tunestream_full_etl_dag",
    start_date=pendulum.datetime(2025, 7, 20, tz="UTC"),
    
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "catchup": False,
        "email_on_retry": False,
    },
    schedule=None,  
    doc_md="""
    ###  ETL Pipeline
    we have the  ETL process:
    -loads JSON data from local files into staging tables.
    -. transforms data from staging into a star-schema data warehouse.
    """,
    tags=["tunestream", "production"],
)
def tunestream_full_etl_dag():
    begin_execution = EmptyOperator(task_id="begin_execution")

    @task(task_id="stage_songs")
    def stage_songs_task():
        song_columns = ["num_songs","artist_id","artist_latitude","artist_longitude","artist_location","artist_name","song_id","title","duration","year"]
        json_to_sql("postgres_local","staging_songs",SONG_DATA_PATH,song_columns)

    @task(task_id="stage_events")
    def stage_events_task():
        event_columns = ["artist","auth","firstName","gender","itemInSession","lastName","length","level","location","method","page","registration","sessionId","song","status","ts","userAgent","userId"]
        json_to_sql("postgres_local","staging_events",LOG_DATA_PATH,event_columns)

    @task(task_id="load_songplays_fact_table")
    def load_songplays_fact_task():
        load_table("postgres_local", "songplays", SqlQueries.songplay_table_insert)

    @task(task_id="load_user_dim_table")
    def load_user_dim_task():
        load_table("postgres_local", "users", SqlQueries.user_table_insert)
    @task(task_id="load_song_dim_table")
    def load_song_dim_task():
        load_table("postgres_local", "songs", SqlQueries.song_table_insert)

    @task(task_id="Load_artist_dim_table")
    def load_artist_dim_task():
        load_table("postgres_local", "artists", SqlQueries.artist_table_insert)

    @task(task_id="Load_time_dim_table")
    def load_time_dim_task():
        load_table("postgres_local", "time", SqlQueries.time_table_insert)

    @task
    def Run_data_quality_checks():

        postgres_hook = PostgresHook(postgres_conn_id="postgres_local")
        
        quality_checks = [
            #check for  null values 
            {'sql': "select COUNT(*) as null_rows from airflow.artists where artistid is null or name is null or location is null or lattitude is null or longitude is null", 'expected_result': 0},
            {'sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL or title is null or artistid is null or year is null or duration is null ", 'expected_result': 0},
            {'sql': "select COUNT(*) as null_rows from airflow.songplays where playid is null or start_time is null or userid is null or level is null or songid is null or artistid is null or sessionid is null or location is null or user_agent is null", 'expected_result': 0},
            {'sql': 'select count(*) as null_rows from airflow."time" where start_time is null or hour is null or day is null or week is null or month is null or year is null or weekday is null', 'expected_result': 0},
            {'sql': "select count(*) as null_rows from airflow.users where userid is null or first_name is null or last_name is null or gender is null or level is null", 'expected_result': 0},
            
            # check for duplicate primary keys in dimension tables
            {'sql': "select COUNT(*) from (select userid, COUNT(*) from users group by userid having COUNT(*) > 1) as duplicates", 'expected_result': 0},
            {'sql': "select COUNT(*) from (select songid, COUNT(*) from songs group by songid having COUNT(*) > 1) as duplicates", 'expected_result': 0},
            {'sql': "select COUNT(*) from (select artistid, COUNT(*) from artists group by artistid having COUNT(*) > 1) as duplicates", 'expected_result': 0},
        ]
        
        errors_found = []
        for check in quality_checks:
            sql = check['sql']
            expected_result = check['expected_result']
            
            print(f"check: {sql}")
            records = postgres_hook.get_records(sql)
            
            if not records or not records[0]:
                raise AirflowException(f"check failed,returned no results: {sql}")

            actual_result = records[0][0]
            if actual_result != expected_result:
                error_message = f"""
                Data quality check failed!
                SQL: "{sql}"
                Expected result: {expected_result}
                Actual result:   {actual_result}
                """
                errors_found.append(error_message)

        if errors_found:

            print("PROBLEMS FOUND, REFER TO ABOVE ERROR MESSAGES. REQUEST IMMEDIATE ATTENTION 😨😨😨😨😨😱😱🥵🥵🥵")
        else:
            print("all checked no error herre")

    end_execution = EmptyOperator(task_id="end_execution")





####$ so here we have all the operations
    stage_songs = stage_songs_task()
    stage_events = stage_events_task()
    load_songplays_fact = load_songplays_fact_task()
    load_user_dim = load_user_dim_task()
    load_song_dim = load_song_dim_task()
    load_artist_dim = load_artist_dim_task()
    load_time_dim = load_time_dim_task()
    run_quality_checks_task = Run_data_quality_checks()
    
    begin_execution >> [stage_songs, stage_events]
    # begin_execution >> stage_songs 
    # begin_execution >> stage_events
    

    [stage_songs, stage_events] >> load_songplays_fact

    load_songplays_fact >> [load_user_dim, load_song_dim, load_artist_dim, load_time_dim]

    [load_user_dim, load_song_dim, load_artist_dim, load_time_dim] >> run_quality_checks_task 
    run_quality_checks_task >> end_execution





tunestream_full_etl_dag()