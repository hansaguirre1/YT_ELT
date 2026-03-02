from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import get_chanel_id, get_playlist_id, get_video_ids, extract_video_data, save_to_json

#Define the local timezone
locatl_tz=pendulum.timezone("America/Lima")

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality


#Default_args
default_args={
    "owner":"dataenginieers",
    "depends on past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "hansaguirre10@gmail.com",
    "max_activate_runs":1,
    "dagrun_timeout":timedelta(hours=1),
    "start_date":datetime(2025,1,1, tzinfo=locatl_tz)
}

#Variables
staging_schema="staging"
core_schema="core"

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule='0 14 * * *',
    catchup=False
) as dag_produce:

    #Define tasks
    channelId=get_chanel_id()
    playlistId=get_playlist_id(channelId)
    video_ids=get_video_ids(playlistId)
    extract_data=extract_video_data(video_ids)
    save_to_json_task=save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="Trigger_update_db",
        trigger_dag_id="update_db"
    )
    #Define Dependecies
    channelId >> playlistId >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description="DAG to process JSON file and insert data into bioth staging and core schemas",
    schedule=None,
    catchup=False
) as dag_update:

    #Define tasks
    update_staging=staging_table()
    update_core=core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="Trigger_data_quality",
        trigger_dag_id="data_quality"
    )
    #second DAG
    update_staging>> update_core >> trigger_data_quality

# DAG 3: data_quality
with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="DAG to check the data quality on both layers in the database",
    catchup=False,
    schedule=None,
) as dag_quality:

    # Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)

    # Define dependencies
    soda_validate_staging >> soda_validate_core