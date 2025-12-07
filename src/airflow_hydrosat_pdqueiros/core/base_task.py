import os

from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.core.documents.asset_data_document import AssetDataDocument
from pathlib import Path
import json


class BaseTask():
    def __init__(self):
        self.s3_client = ClientS3()

    def download_data_with_lock(self, s3_path, local_output_folder: str) -> list[str]:
        '''
        Downloads data from a given S3 folder into a local folder. A limit can be passed to allow for distributed work across multiple containers

        @param: s3_input_folder the S3 folder path
        @param: local_output_folder the local folder path
        @param: limit the number of files to download, defaults to None for no limit.
        '''

        # we lock the file to avoid other processes catching it again
        try:
            locked_s3_path = self.s3_client.lock_file(s3_path=s3_path)
        except Exception as e:
            logger.error(f'Failed to lock file, skipping {s3_path} due to {e}')
        try:
            locked_local_path = self.s3_client.download_file(s3_path=locked_s3_path,
                                                             output_folder=local_output_folder)
            local_path = self.s3_client.get_unlocked_file_path(locked_local_path)
            os.rename(locked_local_path, local_path)
        except Exception as e:
            self.s3_client.unlock_file(locked_s3_path=s3_path)
            logger.error(f'Failed to download {s3_path} due to {e}')


    def process_task(self, asset_data_document: AssetDataDocument, s3_output_path: str):
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
                asset_document = asset_data_document.document_class.from_dict(data=data)
                if asset_document:
                    if asset_document.is_valid():
                        asset_document.process()
                        file.write(f'{json.dumps(asset_document.to_dict())}\n')
        s3_client.upload_file(local_path=asset_data_document.local_output_file_path,
                              s3_path=s3_output_path)
        self.s3_client.move_file(current_path=locked_s3_path, new_path=asset_data_document.archived_s3_path)
        for file_type, file_path in (
            ('input', asset_data_document.local_input_file_path),
            ('output', asset_data_document.local_output_file_path)
            ):
            try:
                os.remove(file_path)
                logger.debug(f"Deleted temp {file_type} file {file_path}")
            except Exception as e:
                logger.error(f"Failed to delete temp {file_type} file {file_path}: {e}")