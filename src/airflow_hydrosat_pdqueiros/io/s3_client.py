import os
import re
from pathlib import Path
from datetime import datetime

import boto3

from airflow_hydrosat_pdqueiros.services.io.logger import logger
from airflow_hydrosat_pdqueiros.services.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_DEFAULT_REGION,
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
    def __init__(self,
                 aws_access_key_id: str=AWS_ACCESS_KEY_ID,
                 aws_secret_access_key: str=AWS_SECRET_ACCESS_KEY,
                 region_name: str=AWS_DEFAULT_REGION,
                 # Im assuming we only use one bucket
                 bucket_name: str=S3_BUCKET):
        self.__aws_access_key_id = aws_access_key_id
        self.__aws_secret_access_key = aws_secret_access_key
        self.__region_name = region_name
        self.__region_name = region_name
        self.__bucket_name = bucket_name
        self.__client = boto3.client('s3',
                                     aws_access_key_id=self.__aws_access_key_id,
                                     aws_secret_access_key=self.__aws_secret_access_key,
                                     region_name=self.__region_name
                                    )
        self.test_s3_connection()

    def test_s3_connection(self):
        try:
            self.__client.head_bucket(Bucket=self.__bucket_name)
            logger.debug(f"Connected to S3 bucket: {self.__bucket_name}")
            return True
        except Exception as e:
            raise Exception(f"Error accessing bucket {self.__bucket_name} due to: {e}") from e

    def get_files(self, prefix: str, file_name_pattern: str, match_on_s3_path: bool=False) -> list[str]:
        res = []
        try:
            response = self.__client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
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
        return res

    def get_input_bounding_boxes(self) -> list[str]:
        return self.get_files(prefix=BOXES_FOLDER_INPUT, file_name_pattern=BOXES_PATTERN)

    def get_output_bounding_boxes(self) -> list[str]:
        return self.get_files(prefix=BOXES_FOLDER_OUTPUT, file_name_pattern=BOXES_PATTERN)

    def get_input_fields(self) -> list[str]:
        return self.get_files(prefix=FIELDS_FOLDER_INPUT, file_name_pattern=FIELDS_PATTERN)

    def get_output_fields(self) -> list[str]:
        return self.get_files(prefix=FIELDS_FOLDER_OUTPUT, file_name_pattern=FIELDS_PATTERN)


    def download_file(self, s3_path: str, output_folder: str) -> str:
        '''
        returns path of the downloaded file
        '''
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        file_name = Path(s3_path).name
        local_path = os.path.join(output_folder, file_name)
        self.__client.download_file(self.__bucket_name, s3_path, local_path)
        return local_path

    def upload_file(self, local_path: str, s3_path: str) -> None:
        '''
        Uploads a local file to S3 at the given s3_path.
        '''
        self.__client.upload_file(Filename=local_path, Bucket=self.__bucket_name, Key=s3_path)

    def file_exists(self, s3_path: str) -> bool:
        try:
            self.__client.get_object(Bucket=self.__bucket_name, Key=s3_path)
            return True
        except Exception as _:
            return False

    def get_all_dates(self) -> list[datetime]:
        raise Exception


if __name__ == '__main__':
    client = ClientS3()
    # print(client.file_exists('boxes/output/bounding_box_01976a1225ca7e32a2daad543cb4391e.jsonl'))
    # print(client.get_files(FIELDS_FOLDER_OUTPUT, file_name_pattern='fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-04-BATCH_2.jsonl', match_on_s3_path=True))

    print(client.get_input_fields())
