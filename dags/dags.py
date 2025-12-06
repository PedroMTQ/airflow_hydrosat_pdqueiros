import logging
import os
import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Variable
from dotenv import dotenv_values
from airflow.models.baseoperator import chain # Use chain for clarity
from airflow.decorators import task # Import the task decorator
from airflow.sdk import dag



IMAGE_NAME = 'airflow-hydrosat-pdqueiros'
ENV_FILE = '/app/.env'
logger = logging.getLogger(__name__)
ENV_CONFIG = dotenv_values(ENV_FILE)
BUCKET_NAME = ENV_CONFIG['S3_BUCKET']
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '10'))
PATH_TO_PYTHON_BINARY = os.getenv('PATH_TO_PYTHON_BINARY')



@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_fields'],
)
def process_fields_dag():
    sensor_key_with_regex = S3KeySensor(task_id="sensor_key_with_regex.process_data",
                                        aws_conn_id=Variable.get('MINIO_CONNECTION', default='minio_connection'),
                                        bucket_name=BUCKET_NAME,
                                        bucket_key=ENV_CONFIG['SENSOR__FIELD_PATTERN'],
                                        timeout=SENSOR_TIMEOUT,
                                        soft_fail=True,
                                        use_regex=True)
    @task.external_python(python=PATH_TO_PYTHON_BINARY, multiple_outputs=True)
    def get_list_fields_tasks() -> list[dict]:
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_fields_tasks
        return get_fields_tasks()

    @task.external_python(python=PATH_TO_PYTHON_BINARY)
    def process_field_worker(task: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_field_task
        print(f"Field dictionary contents: {task}")
        process_field_task(task=task)


    s3_paths = get_list_fields_tasks()
    processing_tasks = process_field_worker.expand(task=s3_paths)
    chain(
            sensor_key_with_regex,
            s3_paths,
            processing_tasks
        )


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_bounding_boxes'],
)
def process_bounding_boxes_dag():
    # TODO same as s3_client.get_input_fields()
    sensor_key_with_regex = S3KeySensor(task_id="sensor_key_with_regex.process_data",
                                        aws_conn_id=Variable.get('MINIO_CONNECTION', default='minio_connection'),
                                        bucket_name=BUCKET_NAME,
                                        bucket_key=ENV_CONFIG['SENSOR__BOUNDING_BOX_PATTERN'],
                                        timeout=SENSOR_TIMEOUT,
                                        soft_fail=True,
                                        use_regex=True)

    @task.external_python(python=PATH_TO_PYTHON_BINARY, multiple_outputs=True)
    def get_list_bounding_boxes_tasks() -> list[dict]:
        from airflow_hydrosat_pdqueiros.core.get_tasks import get_bounding_boxes_tasks
        return get_bounding_boxes_tasks()

    # TODO use a docker operator here instead for POC
    @task.docker(multiple_outputs=True,
                # I'd prefer to use run_id for traceability, but it has forbidden characters -> see this https://stackoverflow.com/questions/63138577/airflow-grab-and-sanitize-run-id-for-dockeroperator
                image=IMAGE_NAME,
                private_environment  = dotenv_values(ENV_FILE),
                api_version='1.51',
                # api_version='auto',
                network_mode="helical-network",
                auto_remove='force',
                # for docker in docker (tecnativa/docker-socket-proxy:v0.4.1) -> https://github.com/benjcabalona1029/DockerOperator-Airflow-Container/tree/master
                mount_tmp_dir=False,
                docker_url="tcp://airflow-docker-socket:2375",
                )
    def process_bounding_box_worker(task: dict):
        from airflow_hydrosat_pdqueiros.core.process_task import process_field_task
        print(f"Field dictionary contents: {task}")
        process_field_task(task=task)


    s3_paths = get_list_bounding_boxes_tasks()
    processing_tasks = process_bounding_box_worker.expand(task=s3_paths)
    chain(
            sensor_key_with_regex,
            s3_paths,
            processing_tasks
        )



process_fields_dag()
process_bounding_boxes_dag()
