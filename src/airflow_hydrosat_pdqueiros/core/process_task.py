import json
import os
from pathlib import Path


from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from airflow_hydrosat_pdqueiros.core.documents.field_document import FieldDocument
from airflow_hydrosat_pdqueiros.settings import (
    BOXES_FOLDER_INPUT,
    BOXES_FOLDER_OUTPUT,
    FIELDS_FOLDER_INPUT,
    FIELDS_FOLDER_OUTPUT,
    TEMP,
)



def process_task(asset_data_document: AssetDataDocument, s3_output_path: str):
    s3_client = ClientS3()
    s3_client.download_file(s3_path=asset_data_document.s3_path,
                            output_folder=asset_data_document.local_input_folder_path)
    Path(asset_data_document.local_input_folder_path).mkdir(parents=True, exist_ok=True)
    Path(asset_data_document.local_output_folder_path).mkdir(parents=True, exist_ok=True)
    with open(asset_data_document.local_output_file_path, 'w+') as file:
        for line in open(asset_data_document.local_input_file_path):
            data = json.loads(line)
            asset_document = asset_data_document.document_class.from_dict(data=data)
            if asset_document:
                if asset_document.is_valid():
                    asset_document.process()
                    file.write(f'{json.dumps(asset_document.to_dict())}\n')
    s3_client.upload_file(local_path=asset_data_document.local_output_file_path,
                            s3_path=s3_output_path)

    for file_type, file_path in (
        ('input', asset_data_document.local_input_file_path),
        ('output', asset_data_document.local_output_file_path)
        ):
        try:
            os.remove(file_path)
            logger.debug(f"Deleted temp {file_type} file {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete temp {file_type} file {file_path}: {e}")


def process_field_task(task: dict):
    '''
    field_taks: {'s3_path': s3_path, 'box_id': box_id, 'date_str': date_str}
    '''
    s3_path = task['s3_path']
    box_id = task['box_id']
    asset_data_document = AssetDataDocument(s3_path=s3_path,
                                            local_input_folder_path=os.path.join(TEMP, FIELDS_FOLDER_INPUT, box_id),
                                            local_output_folder_path=os.path.join(TEMP, FIELDS_FOLDER_OUTPUT, box_id),
                                            document_type=FieldDocument.__name__)
    s3_output_path = os.path.join(FIELDS_FOLDER_OUTPUT, box_id, asset_data_document.file_name)
    process_task(asset_data_document=asset_data_document, s3_output_path=s3_output_path)


def process_bounding_box_task(task: dict):
    '''
    task: {'s3_path': s3_path, 'box_id': box_id}
    '''
    s3_path = task['s3_path']
    asset_data_document = AssetDataDocument(s3_path=s3_path,
                                            local_input_folder_path=os.path.join(TEMP, BOXES_FOLDER_INPUT),
                                            local_output_folder_path=os.path.join(TEMP, BOXES_FOLDER_OUTPUT),
                                            document_type=BoundingBoxDocument.__name__)
    s3_output_path = os.path.join(BOXES_FOLDER_OUTPUT, asset_data_document.file_name)
    process_task(asset_data_document=asset_data_document, s3_output_path=s3_output_path)
