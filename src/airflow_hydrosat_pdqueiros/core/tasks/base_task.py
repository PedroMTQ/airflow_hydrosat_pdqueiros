import os

from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from pathlib import Path
import json


class BaseTask():
    def __init__(self):
        self.s3_client = ClientS3()

    def run(self, asset_data_document: AssetDataDocument, s3_output_path: str):
        try:
            logger.info(f'Processing {asset_data_document}')
            self.process_task(asset_data_document=asset_data_document, s3_output_path=s3_output_path)
            logger.info(f'Processed {asset_data_document}')

        except (KeyboardInterrupt,Exception) as e:
            self.s3_client.unlock_files_on_exception()
            raise e

