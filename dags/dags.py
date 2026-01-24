import os

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.operator import chain
from dotenv import dotenv_values
# plugin is reachable like this, a bit strange but it is what it is
from email_callback import send_email_callback

IMAGE_NAME = 'airflow-hydrosat-pdqueiros'
APP_FOLDER = '/opt/airflow/app/'
ENV_FILE = os.path.join(APP_FOLDER, '.env')
ENV_CONFIG = dotenv_values(ENV_FILE)
BUCKET_NAME = ENV_CONFIG['S3_BUCKET']
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '60'))

DOCKER_BASE_CONFIG = {
    "image": IMAGE_NAME,
    "private_environment": ENV_CONFIG,
    "api_version": '1.51',
    "network_mode": "hydrosat-network",
    "auto_remove": 'force',
    "mount_tmp_dir": False,
    # for docker in docker (tecnativa/docker-socket-proxy:v0.4.1) -> https://github.com/benjcabalona1029/DockerOperator-Airflow-Container/tree/master
    "docker_url": "tcp://airflow-docker-socket:2375",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=1),
}


def create_s3_sensor(task_suffix, file_pattern):
    """
    Factory function to generate S3KeySensors based on a template.
    """
    return S3KeySensor(
        task_id=f"sensor_{task_suffix}",
        aws_conn_id=Variable.get('MINIO_CONNECTION', default='minio_connection'),
        bucket_name=BUCKET_NAME,
        bucket_key=file_pattern,
        timeout=SENSOR_TIMEOUT,
        soft_fail=True,
        use_regex=True,
        poke_interval=10,
        exponential_backoff=True,
    )

@dag(
    schedule='0 * * * *',
    default_args={"on_failure_callback": send_email_callback},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_fields'],
    max_active_tasks=ENV_CONFIG.get('max_active_tasks', 5),
    doc_md='''
    Processes fields data by:
    1. Fetches data that needs to be processed
    2. Locks files so that no other worker can process it
    3. Downloads files
    4. Generates output files and uploads them to fields/output
    5. Moves input files to fields/archived_input
    '''
)
def process_fields_dag():
    sensor = create_s3_sensor(task_suffix='check_fields', file_pattern=ENV_CONFIG['SENSOR__FIELD_PATTERN'])

    @task.docker(**DOCKER_BASE_CONFIG, task_id="get_list_fields")
    def get_list_fields_tasks():
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_fields_tasks
        return get_fields_tasks()

    @task.docker(**DOCKER_BASE_CONFIG, task_id="process_field")
    def process_field_worker(task_data: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_field_task
        print(f"Task data: {task_data}")
        process_field_task(task_data=task_data)

    task_data_list = get_list_fields_tasks()
    processing_tasks = process_field_worker.expand(task_data=task_data_list)
    chain(
            sensor,
            task_data_list,
            processing_tasks
        )


@dag(
    schedule='0 * * * *',
    default_args={"on_failure_callback": send_email_callback},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_bounding_boxes'],
    max_active_tasks=ENV_CONFIG.get('max_active_tasks', 5)
)
def process_bounding_boxes_dag():
    sensor = create_s3_sensor(task_suffix='check_bounding_boxes', file_pattern=ENV_CONFIG['SENSOR__BOUNDING_BOX_PATTERN'])
    @task.docker(**DOCKER_BASE_CONFIG, task_id="get_list_bboxes")
    def get_list_bounding_boxes_tasks():
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_bounding_boxes_tasks
        return get_bounding_boxes_tasks()

    @task.docker(**DOCKER_BASE_CONFIG, task_id='process_bbox')
    def process_bounding_box_worker(task_data: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_bounding_box_task
        print(f"Task data: {task_data}")
        process_bounding_box_task(task_data=task_data)


    task_data_list = get_list_bounding_boxes_tasks()
    processing_tasks = process_bounding_box_worker.expand(task_data=task_data_list)
    chain(
            sensor,
            task_data_list,
            processing_tasks
        )



process_fields_dag()
process_bounding_boxes_dag()
