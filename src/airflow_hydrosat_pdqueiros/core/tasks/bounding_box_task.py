import os

from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.core.tasks.base_task import BaseTask
from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from pathlib import Path
import json


class BoundingBoxTask(BaseTask):
    def __init__(self):
        super().__init__()

    def process_task(self, asset_data_document: AssetDataDocument, s3_output_path: str):
        '''
        {"box_id": "01978c3831bc710c9e0663456e70de1e",
          "coordinates_x_min": 59,
            "coordinates_y_min": 48,
              "coordinates_x_max": 127,
                "coordinates_y_max": 81,
                  "irrigation_array": [[1, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0], [1, 0, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0],...]
                  "is_processed": false}
        '''

        s3_client = ClientS3()
        locked_s3_path = self.s3_client.lock_file(s3_path=asset_data_document.s3_path)
        Path(asset_data_document.local_input_folder_path).mkdir(parents=True, exist_ok=True)
        Path(asset_data_document.local_output_folder_path).mkdir(parents=True, exist_ok=True)
        locked_local_path = s3_client.download_file(s3_path=locked_s3_path,
                                                    output_folder=asset_data_document.local_input_folder_path)
        os.rename(locked_local_path, asset_data_document.local_input_file_path)
        with open(asset_data_document.local_output_file_path, 'w+') as file:
            for line in open(asset_data_document.local_input_file_path):
                data = json.loads(line)
                try:
                    asset_document = BoundingBoxDocument(**data)
                except Exception as e:
                    logger.exception(e)
                    continue
                asset_document.process()
                file.write(f'{json.dumps(asset_document.model_dump())}\n')
        s3_client.upload_file(local_path=asset_data_document.local_output_file_path,
                              s3_path=s3_output_path)
        self.s3_client.move_file(current_path=locked_s3_path, new_path=asset_data_document.archived_s3_path)
        asset_data_document.delete_local()
