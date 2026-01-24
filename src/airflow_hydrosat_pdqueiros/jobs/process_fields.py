import logging
from time import sleep

from airflow_hydrosat_pdqueiros.settings import SLEEP_TIME
from airflow_hydrosat_pdqueiros.core.process_task import process_field_task
from airflow_hydrosat_pdqueiros.core.get_tasks import get_fields_tasks

logger = logging.getLogger(__name__)


class ProcessFieldsJob():

    def run(self):
        task_data_list = get_fields_tasks()
        logger.info(f'List of tasks: {task_data_list}')
        if not task_data_list:
            logger.info('Terminating job since no data was found...')
            return
        for task_data in task_data_list:
            logger.info(f'Processing {task_data}')
            process_field_task(task_data=task_data)
            sleep(SLEEP_TIME)

if __name__ == '__main__':
    job = ProcessFieldsJob()
    job.run()




