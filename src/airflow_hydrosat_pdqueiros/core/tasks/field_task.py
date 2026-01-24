import os

from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from airflow_hydrosat_pdqueiros.core.documents.field_document import FieldDocument
from pathlib import Path
import json
from airflow_hydrosat_pdqueiros.core.tasks.base_task import BaseTask
from airflow_hydrosat_pdqueiros.settings import (
    BOXES_FOLDER_OUTPUT,
    TEMP,
)

class FieldTask(BaseTask):
    def __init__(self):
        super().__init__()

    def process_task(self, asset_data_document: AssetDataDocument, s3_output_path: str):
        '''
        {"box_id": "01978c3831bc710c9e0663456e70de1e", "coordinates_x_min": 4, "coordinates_y_min": 7, "coordinates_x_max": 8, "coordinates_y_max": 12,
          "irrigation_array": [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]],
            "is_processed": false}

        in order to irrigate the field, we need to get the data on the irrigation status of the fields respective bounding box. There could be an intersection, but for now we kept them independent
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
                    asset_document = FieldDocument(**data)
                except Exception as e:
                    logger.exception(e)
                    continue
                try:
                    box_id = data['box_id']
                    logger.debug(f'Getting path for bounding box {box_id} for {asset_data_document}')
                    bounding_box_s3_path = s3_client.get_files(prefix=BOXES_FOLDER_OUTPUT,
                                                               file_name_pattern=f'bounding_box_{box_id}.jsonl',
                                                               match_on_s3_path=False)
                    bounding_box_s3_path = bounding_box_s3_path[0]
                    logger.debug(f'Downloading bounding box for {asset_data_document} from {bounding_box_s3_path}')
                    local_bounding_box_file = s3_client.download_file(s3_path=bounding_box_s3_path,
                                                                      output_folder=os.path.join(TEMP, BOXES_FOLDER_OUTPUT, Path(bounding_box_s3_path).name))
                    logger.debug(f'Local bounding box file path: {local_bounding_box_file}')
                    bounding_box_document = BoundingBoxDocument(**json.loads(next(open(local_bounding_box_file))))
                    logger.debug(f'Bounding box document: {bounding_box_document}')
                    asset_document.irrigate(bounding_box_document=bounding_box_document)
                    os.remove(local_bounding_box_file)
                except Exception as e:
                    logger.error(f'Error while processsing {asset_data_document}: {e}')
                    asset_data_document.errors.append(str(e))
                file.write(f'{json.dumps(asset_document.model_dump())}\n')
        s3_client.upload_file(local_path=asset_data_document.local_output_file_path,
                              s3_path=s3_output_path)
        self.s3_client.move_file(current_path=locked_s3_path, new_path=asset_data_document.archived_s3_path)
        asset_data_document.delete_local()
