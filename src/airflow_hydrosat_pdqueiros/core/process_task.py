import os

from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from airflow_hydrosat_pdqueiros.core.documents.field_document import FieldDocument
from airflow_hydrosat_pdqueiros.settings import (
    BOXES_FOLDER_INPUT,
    BOXES_FOLDER_OUTPUT,
    FIELDS_FOLDER_INPUT,
    FIELDS_FOLDER_OUTPUT,
    TEMP,
)
from airflow_hydrosat_pdqueiros.core.base_task import BaseTask



def process_field_task(task_data: dict):
    '''
    field_taks: {'s3_path': s3_path, 'box_id': box_id, 'date_str': date_str}
    '''
    s3_path = task_data['s3_path']
    box_id = task_data['box_id']
    asset_data_document = AssetDataDocument(s3_path=s3_path,
                                            local_input_folder_path=os.path.join(TEMP, FIELDS_FOLDER_INPUT, box_id),
                                            local_output_folder_path=os.path.join(TEMP, FIELDS_FOLDER_OUTPUT, box_id),
                                            document_type=FieldDocument.__name__)
    s3_output_path = os.path.join(FIELDS_FOLDER_OUTPUT, box_id, asset_data_document.file_name)
    task_processor = BaseTask()
    task_processor.process_task(asset_data_document=asset_data_document, s3_output_path=s3_output_path)


def process_bounding_box_task(task_data: dict):
    '''
    task: {'s3_path': s3_path, 'box_id': box_id}
    '''
    s3_path = task_data['s3_path']
    asset_data_document = AssetDataDocument(s3_path=s3_path,
                                            local_input_folder_path=os.path.join(TEMP, BOXES_FOLDER_INPUT),
                                            local_output_folder_path=os.path.join(TEMP, BOXES_FOLDER_OUTPUT),
                                            document_type=BoundingBoxDocument.__name__)
    s3_output_path = os.path.join(BOXES_FOLDER_OUTPUT, asset_data_document.file_name)
    task_processor = BaseTask()
    task_processor.process_task(asset_data_document=asset_data_document, s3_output_path=s3_output_path)

if __name__ == '__main__':
    task_data = {'box_id': '01978c3831c0772bbd6ad9856cdb3834', 's3_path': 'fields/input/01978c3831c0772bbd6ad9856cdb3834/fields_2025-06-02_01978c3831d47bd5aeca4f0e754c527e.jsonl', 'date_str': '2025-06-01'}
    process_field_task(task_data=task_data)
