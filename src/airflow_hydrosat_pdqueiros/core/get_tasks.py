import os
import re
from datetime import datetime
from pathlib import Path

from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.settings import (
    BOXES_FOLDER_OUTPUT,
    DATE_FORMAT,
    FIELDS_FOLDER_INPUT,
    FIELDS_FOLDER_OUTPUT,
    S3_BUCKET,
    S3_DATE_REGEX,
)

DATE_REGEX_PATTERN = re.compile(S3_DATE_REGEX)


def is_valid_field_run_request(s3_client: ClientS3,
                               s3_path: str,
                               box_id: str,
                               all_dates: list[datetime],
                               date_str: str) -> bool:
    '''checks if all dependencies for a field execution are met'''
    date_obj = datetime.strptime(date_str, DATE_FORMAT)
    earliest_date = min(all_dates)
    # for now we are skipping runs outside of the partition limits
    if date_obj < earliest_date:
        logger.info(f'Field data {s3_path} precedes the earliest partition date, skipping...')
        return False

    output_box_file = os.path.join(BOXES_FOLDER_OUTPUT, f'bounding_box_{box_id}.jsonl')
    if not s3_client.file_exists(output_box_file):
        logger.info(f'Field data {s3_path} skipped since output box file {output_box_file} is not available yet')
        return False
    # if the field is the first one given the date limits, you can go ahead and process
    if date_obj == earliest_date:
        return True
    sorted_dates = sorted(all_dates)
    current_date_index = sorted_dates.index(date_obj)
    previous_date = sorted_dates[current_date_index-1]
    previous_date_str = previous_date.strftime(DATE_FORMAT)
    previous_date_s3_input_file_pattern = rf'fields\/input\/{box_id}\/fields_{previous_date_str}(.*)?\.jsonl$'
    previous_date_s3_output_file_pattern = rf'fields\/output\/{box_id}\/fields_{previous_date_str}(.*)?\.jsonl$'
    # we get the output files of the previous date
    previous_date_output_s3_files = set(s3_client.get_files(prefix=FIELDS_FOLDER_OUTPUT,
                                                            file_name_pattern=previous_date_s3_output_file_pattern,
                                                            match_on_s3_path=True))
    # if there are none we cannot run
    if not previous_date_output_s3_files:
        logger.info(f'Field data {s3_path} depends on data from {previous_date_s3_output_file_pattern}, and the data was NOT found, skipping...')
        return False
    previous_date_input_s3_files = s3_client.get_files(prefix=FIELDS_FOLDER_INPUT,
                                                        file_name_pattern=previous_date_s3_input_file_pattern,
                                                        match_on_s3_path=True)
    # now if there are some but not the same as the input we also cannot run
    if len(previous_date_output_s3_files) != len(previous_date_input_s3_files):
        logger.info(f'Field data {s3_path} depends on data from {previous_date_s3_output_file_pattern}, and only part of the data was found ({len(previous_date_output_s3_files)}/{len(previous_date_s3_input_file_pattern)}), skipping...')
        return False
    return True



def get_fields_tasks() -> list[dict]:
    logger.info("Running s3_check_sensor")
    s3_client = ClientS3()
    s3_file_paths = []
    valid_s3_file_paths = []
    try:
        s3_file_paths = s3_client.get_input_fields()
    except Exception as e:
        logger.exception(e)
        return valid_s3_file_paths
    if not s3_file_paths:
        logger.info(f'No file found in {os.path.join(S3_BUCKET, FIELDS_FOLDER_INPUT)}')
        return valid_s3_file_paths
    logger.debug(f'Files in S3: {s3_file_paths}')
    all_dates = [DATE_REGEX_PATTERN.search(s3_path).group() for s3_path in s3_file_paths]
    all_dates_objects = [datetime.strptime(date_str, DATE_FORMAT) for date_str in all_dates if date_str]
    for s3_path in s3_file_paths:
        # assuming this structure fields/input/01976a1225ca7e32a2daad543cb4391e/fields_2025-06-01.jsonl
        box_id = Path(Path(s3_path).parent).name
        date_str = DATE_REGEX_PATTERN.search(s3_path).group()
        if is_valid_field_run_request(s3_client=s3_client,
                                      s3_path=s3_path,
                                      box_id=box_id,
                                      all_dates=all_dates_objects,
                                      date_str=date_str):
            valid_s3_file_paths.append({'s3_path': s3_path, 'box_id': box_id, 'date_str': date_str})
    if not valid_s3_file_paths:
        logger.info('Skipping since fields data does not meet job dependencies')
    return valid_s3_file_paths


def get_bounding_boxes_tasks() -> list[dict]:
    logger.info("Running s3_check_sensor")
    s3_client: ClientS3 = ClientS3()
    s3_file_paths = []
    s3_file_paths = []
    valid_s3_file_paths = []
    try:
        s3_file_paths = s3_client.get_input_bounding_boxes()
    except Exception as e:
        logger.exception(e)
        return valid_s3_file_paths
    if not s3_file_paths:
        logger.info(f'No file found in {os.path.join(S3_BUCKET, FIELDS_FOLDER_INPUT)}')
        return valid_s3_file_paths
    logger.info(f'Files in S3: {s3_file_paths}')
    for s3_path in s3_file_paths:
        box_id = Path(s3_path).stem.replace('bounding_box_', '')
        valid_s3_file_paths.append({'s3_path': s3_path, 'box_id': box_id})
    if not valid_s3_file_paths:
        logger.info('Skipping since boxes files fall outside the required partitions')
    return valid_s3_file_paths

if __name__ == '__main__':
    fields_tasks = get_fields_tasks()
    bounding_boxes_tasks = get_bounding_boxes_tasks()
    print(f'field_tasks', fields_tasks)
    print(f'bounding_boxes_tasks', bounding_boxes_tasks)