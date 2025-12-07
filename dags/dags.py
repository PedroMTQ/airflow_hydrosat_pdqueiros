import os

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.operator import chain
from dotenv import dotenv_values

IMAGE_NAME = 'airflow-hydrosat-pdqueiros'
APP_FOLDER = '/opt/airflow/app/'
ENV_FILE = os.path.join(APP_FOLDER, '.env')
ENV_CONFIG = dotenv_values(ENV_FILE)
BUCKET_NAME = ENV_CONFIG['S3_BUCKET']
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '10'))


@dag(
    schedule='* * * * *',
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
            sensor_key_with_regex,
            # I'd only create an env if this env could be semi-permanent and used globally. Otherwise I'd probably use a Docker operator for better release management.
            # In any case, this is a good proof-of-concept
            create_env_task,
            s3_paths,
            processing_tasks
        )


@dag(
    schedule='* * * * *',
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
            sensor_key_with_regex,
            s3_paths,
            processing_tasks
        )



process_fields_dag()
process_bounding_boxes_dag()
