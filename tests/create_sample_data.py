import json
import os
import random
from pathlib import Path

import uuid6

from airflow_hydrosat_pdqueiros.services.core.documents.bounding_box_document import BoundingBoxDocument
from airflow_hydrosat_pdqueiros.services.core.documents.field_document import FieldDocument
from airflow_hydrosat_pdqueiros.services.io.s3_client import ClientS3
from airflow_hydrosat_pdqueiros.services.settings import BOXES_FOLDER_INPUT, FIELDS_FOLDER_INPUT, TESTS


def get_uuid():
    return uuid6.uuid7().hex

# this is initialized
RANDOM_BOX_IDS = [get_uuid() for _ in range(5)]

# we use this for partitioning the data, including the field. Each field is associated with tha bounding box, although in reality this would be much more complex
def get_box_id():
    return random.choice(RANDOM_BOX_IDS)

FIELDS_FOLDER = os.path.join(TESTS, 'data', FIELDS_FOLDER_INPUT)
BOXES_FOLDER = os.path.join(TESTS, 'data', BOXES_FOLDER_INPUT)


def generate_random_coordinates(max_int: int):
    base_x = random.randint(1, max_int)
    base_y = random.randint(1, max_int)
    width = random.randint(1, max_int)
    height = random.randint(1, max_int)
    return base_x, base_y, base_x + width, base_y + height

def generate_fields_files():
    Path(FIELDS_FOLDER)
    for day in range(1,6):
        if day < 10:
            day_str = f'0{day}'
        sample_date = f"2025-06-{day_str}"
        for _ in range(random.choice([1,2,3])):
            file_name = f'fields_{sample_date}_{get_uuid()}.jsonl'
            # we allocate a field to each box for simplicity sake -> later used for partitioning the assets
            for _ in range(random.randint(10,30)):
                box_id = get_box_id()
                field_data = [*generate_random_coordinates(max_int=10)] + [box_id]
                data = FieldDocument(*field_data).to_dict()
                Path(os.path.join(FIELDS_FOLDER, box_id)).mkdir(parents=True, exist_ok=True)
                with open(os.path.join(FIELDS_FOLDER, box_id, file_name), 'a+') as file:
                    file.write(f'{json.dumps(data)}\n')

def generate_bounding_box_files():
    Path(BOXES_FOLDER).mkdir(parents=True, exist_ok=True)
    for box_id in RANDOM_BOX_IDS:
        file_name = f'bounding_box_{box_id}.jsonl'
        box_data = [*generate_random_coordinates(max_int=100)] + [box_id]
        data = BoundingBoxDocument(*box_data).to_dict()
        with open(os.path.join(BOXES_FOLDER, file_name), 'w+') as file:
            file.write(f'{json.dumps(data)}\n')

def upload_to_s3():
    s3_client = ClientS3()
    for box_id in os.listdir(FIELDS_FOLDER):
        for file in os.listdir(os.path.join(FIELDS_FOLDER, box_id)):
            s3_client.upload_file(local_path=os.path.join(FIELDS_FOLDER, box_id, file),
                                  s3_path=os.path.join(FIELDS_FOLDER_INPUT,box_id, file))
    for file in os.listdir(BOXES_FOLDER):
        s3_client.upload_file(local_path=os.path.join(BOXES_FOLDER, file),
                              s3_path=os.path.join(BOXES_FOLDER_INPUT, file))


if __name__ == "__main__":
    generate_bounding_box_files()
    generate_fields_files()
    # upload_to_s3()
