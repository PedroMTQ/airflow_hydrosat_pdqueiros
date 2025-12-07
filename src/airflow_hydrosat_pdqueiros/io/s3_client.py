import os
import re
from pathlib import Path
from botocore.client import Config

import boto3
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.settings import (
    AWS_ACCESS_KEY_ID,
    MINIO_HOST,
    MINIO_PORT,
    AWS_SECRET_ACCESS_KEY,
    BOXES_FOLDER_INPUT,
    BOXES_FOLDER_OUTPUT,
    BOXES_PATTERN,
    FIELDS_FOLDER_INPUT,
    FIELDS_FOLDER_OUTPUT,
    FIELDS_PATTERN,
    S3_BUCKET,
)





class ClientS3():
    lock_string = '.lock'
    def __init__(self,
                 aws_access_key: str=AWS_ACCESS_KEY_ID,
                 aws_secret_access_key: str=AWS_SECRET_ACCESS_KEY,
                 s3_host: str=MINIO_HOST,
                 s3_port: str=MINIO_PORT,
                 # Im assuming we only use one bucket
                 bucket_name: str=S3_BUCKET):
        self.s3_host = s3_host
        self.s3_port = s3_port
        self.bucket_name = bucket_name
        self.endpoint_url = f'http://{self.s3_host}:{self.s3_port}'
        config = Config(connect_timeout=5, retries={'max_attempts': 2})
        logger.debug(f'Trying to connect to {self.endpoint_url}')
        self.__client = boto3.client('s3',
                                     aws_access_key_id=aws_access_key,
                                     aws_secret_access_key=aws_secret_access_key,
                                     endpoint_url=self.endpoint_url,
                                     config=config)
        self.test_s3_connection()
        self.__locked_files = set()

    def unlock_files_on_exception(self):
        for locked_s3_path in list(self.__locked_files):
            try:
                self.unlock_file(locked_s3_path=locked_s3_path)
                logger.warning(f'Unlocking on exception: {locked_s3_path}')
            except Exception as e:
                logger.error(f'Failed to unlock {locked_s3_path} due to {e}')

    def test_s3_connection(self):
        try:
            self.__client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"Connected to S3 bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket: {self.bucket_name}")
            raise Exception(f"Error accessing bucket {self.bucket_name} due to: {e}") from e

    def get_files(self, prefix: str, file_name_pattern: str, match_on_s3_path: bool=False) -> list[str]:
        res = []
        try:
            response = self.__client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        except Exception as e:
            raise e
        regex_pattern = re.compile(file_name_pattern)
        for obj in response.get("Contents", []):
            s3_path = obj["Key"]
            file_name = Path(s3_path).name
            if regex_pattern.fullmatch(file_name) or (match_on_s3_path and regex_pattern.fullmatch(s3_path)):
                res.append(s3_path)
            else:
                logger.debug(f'File skipped: {s3_path}')
        logger.debug(f'Files returned from {prefix}: {res}')
        return res

    @staticmethod
    def get_locked_file_path(file_path):
        return f'{file_path}{ClientS3.lock_string}'

    @staticmethod
    def get_unlocked_file_path(file_path):
        return file_path.removesuffix(ClientS3.lock_string)

    def lock_file(self, s3_path: str) -> str:
        locked_s3_path = ClientS3.get_locked_file_path(s3_path)
        self.__client.copy_object(Bucket=self.bucket_name, CopySource={'Bucket': self.bucket_name, 'Key': s3_path}, Key=locked_s3_path)
        self.move_file(current_path=s3_path, new_path=locked_s3_path)
        self.__locked_files.add(locked_s3_path)
        return locked_s3_path

    def unlock_file(self, locked_s3_path: str) -> str:
        s3_path = ClientS3.get_unlocked_file_path(locked_s3_path)
        self.move_file(current_path=locked_s3_path, new_path=s3_path)
        if locked_s3_path in self.__locked_files:
            self.__locked_files.remove(locked_s3_path)
        return s3_path

    def delete_file(self, s3_path: str) -> bool:
        try:
            self.__client.delete_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as e:
            logger.exception(f'Failed to delete {s3_path} due to {e}')
            return False

    def move_file(self, current_path: str, new_path: str) -> str:
        try:
            self.__client.copy_object(Bucket=self.bucket_name, CopySource={'Bucket': self.bucket_name, 'Key': current_path}, Key=new_path)
            self.__client.delete_object(Bucket=self.bucket_name, Key=current_path)
        except Exception as e:
            logger.exception(f'Failed to move {current_path} to {new_path} due to {e}')
        return new_path

    def download_file(self, s3_path: str, output_folder: str) -> str:
        '''
        returns path of the downloaded file
        '''
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        file_name = Path(s3_path).name
        local_path = os.path.join(output_folder, file_name)
        self.__client.download_file(self.bucket_name, s3_path, local_path)
        logger.debug(f'Downloaded {s3_path} to {output_folder}')
        return local_path

    def upload_file(self, local_path: str, s3_path: str) -> bool:
        '''
        Uploads a local file to S3 at the given s3_path.
        '''
        try:
            self.__client.upload_file(Filename=local_path, Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as e:
            logger.error(f'Failed to upload {local_path} to {s3_path} due to {e}')
            return False

    def read_file(self, s3_path: str) -> bytes:
        return self.__client.get_object(Bucket=self.bucket_name, Key=s3_path)['Body'].read()

    def file_exists(self, s3_path: str) -> bool:
        try:
            self.__client.get_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as _:
            return False

    def get_input_bounding_boxes(self) -> list[str]:
        return self.get_files(prefix=BOXES_FOLDER_INPUT, file_name_pattern=BOXES_PATTERN)

    def get_output_bounding_boxes(self) -> list[str]:
        return self.get_files(prefix=BOXES_FOLDER_OUTPUT, file_name_pattern=BOXES_PATTERN)

    def get_input_fields(self) -> list[str]:
        return self.get_files(prefix=FIELDS_FOLDER_INPUT, file_name_pattern=FIELDS_PATTERN)

    def get_output_fields(self) -> list[str]:
        return self.get_files(prefix=FIELDS_FOLDER_OUTPUT, file_name_pattern=FIELDS_PATTERN)


if __name__ == '__main__':
    client = ClientS3()
    # print(client.file_exists('boxes/output/bounding_box_01976a1225ca7e32a2daad543cb4391e.jsonl'))
    # print(client.get_files(FIELDS_FOLDER_OUTPUT, file_name_pattern='fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-04-BATCH_2.jsonl', match_on_s3_path=True))
    print(client.get_input_bounding_boxes())
