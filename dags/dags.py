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
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '10'))

def create_s3_sensor(task_suffix, file_pattern):
    """
    Factory function to generate S3KeySensors based on a template.
    """
    return S3KeySensor(
        task_id=f"sensor_key_with_regex.{task_suffix}",
        aws_conn_id=Variable.get('MINIO_CONNECTION', default='minio_connection'),
        bucket_name=BUCKET_NAME,
        bucket_key=file_pattern,
        timeout=SENSOR_TIMEOUT,
        soft_fail=True,
        use_regex=True,
        poke_interval=300,
        exponential_backoff=True,
    )


@dag(
    schedule='0 * * * *',
    default_args={"on_failure_callback": send_email_callback},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_fields'],
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
    @task.bash(task_id="setup_uv_environment", cwd=APP_FOLDER)
    def setup_uv_environment():
        return '/opt/airflow/scripts/create_env.sh '

    create_env_task = setup_uv_environment()
    PATH_TO_PYTHON_BINARY = create_env_task

    @task.external_python(python=PATH_TO_PYTHON_BINARY)
    def get_list_fields_tasks() -> list[dict]:
        from airflow_hydrosat_pdqueiros.io.logger import logger
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_fields_tasks
        list_task_data = get_fields_tasks()
        logger.info(f'Valid fields tasks: {list_task_data}')
        return list_task_data

    @task.external_python(python=PATH_TO_PYTHON_BINARY, do_xcom_push=False)
    def process_field_worker(task_data: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_field_task
        print(f"Processing field: {task_data}")
        process_field_task(task_data=task_data)


    s3_paths = get_list_fields_tasks()
    processing_tasks = process_field_worker.expand(task_data=s3_paths)

    chain(
            sensor,
            # I'd only create an env if this env could be semi-permanent and used globally. Otherwise I'd probably use a Docker operator for better release management.
            # In any case, this is a good proof-of-concept
            create_env_task,
            s3_paths,
            processing_tasks
        )


@dag(
    schedule='0 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_bounding_boxes'],
    max_active_tasks=ENV_CONFIG.get('max_active_tasks', 5)
)
def process_bounding_boxes_dag():
    sensor = create_s3_sensor(task_suffix='check_bounding_boxes', file_pattern=ENV_CONFIG['SENSOR__BOUNDING_BOX_PATTERN'])


    # TODO use a docker operator here instead for POC
    @task.docker(# I'd prefer to use run_id for traceability, but it has forbidden characters -> see this https://stackoverflow.com/questions/63138577/airflow-grab-and-sanitize-run-id-for-dockeroperator
                image=IMAGE_NAME,
                private_environment  = dotenv_values(ENV_FILE),
                api_version='1.51',
                # api_version='auto',
                network_mode="hydrosat-network",
                auto_remove='force',
                # for docker in docker (tecnativa/docker-socket-proxy:v0.4.1) -> https://github.com/benjcabalona1029/DockerOperator-Airflow-Container/tree/master
                mount_tmp_dir=False,
                docker_url="tcp://airflow-docker-socket:2375",
                retries=3,
                retry_delay=pendulum.duration(minutes=1),
                )
    def get_list_bounding_boxes_tasks():
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_bounding_boxes_tasks
        return get_bounding_boxes_tasks()

    @task.docker(image=IMAGE_NAME,
                 private_environment  = dotenv_values(ENV_FILE),
                 api_version='1.51',
                 # api_version='auto',
                 network_mode="hydrosat-network",
                 auto_remove='force',
                 # for docker in docker (tecnativa/docker-socket-proxy:v0.4.1) -> https://github.com/benjcabalona1029/DockerOperator-Airflow-Container/tree/master
                 mount_tmp_dir=False,
                 docker_url="tcp://airflow-docker-socket:2375",
                 do_xcom_push=False,
                )
    def process_bounding_box_worker(task_data: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_bounding_box_task
        print(f"Field dictionary contents: {task_data}")
        process_bounding_box_task(task_data=task_data)


    s3_paths = get_list_bounding_boxes_tasks()
    processing_tasks = process_bounding_box_worker.expand(task_data=s3_paths)
    chain(
            sensor,
            s3_paths,
            processing_tasks
        )



process_fields_dag()
process_bounding_boxes_dag()
