import os

import pendulum
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.operator import chain
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from dotenv import dotenv_values
# plugin is reachable like this, a bit strange but it is what it is
from email_callback import send_email_callback

IMAGE_NAME = 'airflow-hydrosat-pdqueiros'
APP_FOLDER = '/opt/airflow/app/'
ENV_FILE = os.path.join(APP_FOLDER, '.env')
ENV_CONFIG = dotenv_values(ENV_FILE)
BUCKET_NAME = ENV_CONFIG['S3_BUCKET']
SENSOR_TIMEOUT = int(ENV_CONFIG.get('SENSOR_TIMEOUT', '10'))

# Kubernetes configuration - these would typically come from environment variables or config
KUBERNETES_NAMESPACE = ENV_CONFIG.get('KUBERNETES_NAMESPACE', 'default')
KUBERNETES_IMAGE_PULL_SECRETS = ENV_CONFIG.get('KUBERNETES_IMAGE_PULL_SECRETS', None)


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


def create_kubernetes_pod_task(
    task_id: str,
    task_function: str,
    arguments: list = None,
    do_xcom_push: bool = False,
    **kwargs
):
    """
    Factory function to create KubernetesPodOperator tasks.
    
    Args:
        task_id: Unique identifier for the task
        task_function: The function to execute in the pod
        arguments: List of arguments to pass to the task function
        do_xcom_push: Whether to push XCom results
        **kwargs: Additional arguments to pass to KubernetesPodOperator
    """
    if arguments is None:
        arguments = []
    
    # Convert arguments to command-line format
    args_list = ["python", "-c"]
    import_statement = "from airflow_hydrosat_pdqueiros.core.process_task import {}".format(task_function)
    call_statement = "{}({})".format(task_function, ", ".join(arguments) if arguments else "")
    
    # Create the full Python command
    python_command = "{}; {}".format(import_statement, call_statement)
    args_list.append(python_command)
    
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=KUBERNETES_NAMESPACE,
        image=IMAGE_NAME,
        cmds=["python"],
        arguments=args_list,
        name=task_id,
        is_delete_operator_pod=True,
        get_logs=True,
        do_xcom_push=do_xcom_push,
        env_vars={
            'S3_BUCKET': BUCKET_NAME,
            # Add other environment variables as needed
        },
        image_pull_secrets=KUBERNETES_IMAGE_PULL_SECRETS,
        **kwargs
    )


@dag(
    schedule='0 * * * *',
    default_args={"on_failure_callback": send_email_callback},
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_fields_kubernetes'],
    doc_md='''
    Processes fields data using KubernetesPodOperator:
    1. Fetches data that needs to be processed
    2. Locks files so that no other worker can process it
    3. Downloads files
    4. Generates output files and uploads them to fields/output
    5. Moves input files to fields/archived_input
    '''
)
def process_fields_kubernetes_dag():
    sensor = create_s3_sensor(task_suffix='check_fields_k8s', file_pattern=ENV_CONFIG['SENSOR__FIELD_PATTERN'])
    
    # Create a task to get the list of fields tasks using Kubernetes
    get_fields_tasks = create_kubernetes_pod_task(
        task_id="get_list_fields_tasks_k8s",
        task_function="get_fields_tasks",
        arguments=[],
        do_xcom_push=True
    )
    
    # Create a task to process each field using Kubernetes
    def create_process_field_task(task_data: dict):
        return create_kubernetes_pod_task(
            task_id=f"process_field_worker_k8s_{task_data.get('id', 'unknown')}",
            task_function="process_field_task",
            arguments=[f"task_data={task_data}"],
            do_xcom_push=False
        )
    
    # Note: Since KubernetesPodOperator doesn't support .expand() like @task operators,
    # we need to handle the dynamic task creation differently. This is a simplified approach.
    # In a real implementation, you might need to use a different pattern or a custom operator.
    
    # For now, let's create a placeholder for the processing tasks
    # In practice, you would need to implement a more sophisticated approach
    # to handle the dynamic task creation with KubernetesPodOperator
    
    chain(
        sensor,
        get_fields_tasks,
        # processing_tasks would go here
    )


@dag(
    schedule='0 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['process_bounding_boxes_kubernetes'],
    max_active_tasks=ENV_CONFIG.get('max_active_tasks', 5)
)
def process_bounding_boxes_kubernetes_dag():
    sensor = create_s3_sensor(task_suffix='check_bounding_boxes_k8s', file_pattern=ENV_CONFIG['SENSOR__BOUNDING_BOX_PATTERN'])

    # Create a task to get the list of bounding boxes tasks using Kubernetes
    get_bounding_boxes_tasks = create_kubernetes_pod_task(
        task_id="get_list_bounding_boxes_tasks_k8s",
        task_function="get_bounding_boxes_tasks",
        arguments=[],
        do_xcom_push=True
    )
    
    # Create a task to process each bounding box using Kubernetes
    def create_process_bounding_box_task(task_data: dict):
        return create_kubernetes_pod_task(
            task_id=f"process_bounding_box_worker_k8s_{task_data.get('id', 'unknown')}",
            task_function="process_bounding_box_task",
            arguments=[f"task_data={task_data}"],
            do_xcom_push=False
        )
    
    # Similar to the fields DAG, we need to handle dynamic task creation differently
    # for KubernetesPodOperator
    
    chain(
        sensor,
        get_bounding_boxes_tasks,
        # processing_tasks would go here
    )


# Instantiate the DAGs
process_fields_kubernetes_dag()
process_bounding_boxes_kubernetes_dag()