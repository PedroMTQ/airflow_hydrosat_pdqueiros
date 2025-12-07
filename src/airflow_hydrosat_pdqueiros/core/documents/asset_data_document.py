import os
from dataclasses import dataclass, field
from pathlib import Path

from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from airflow_hydrosat_pdqueiros.core.documents.field_document import FieldDocument
from airflow_hydrosat_pdqueiros.io.logger import logger
from airflow_hydrosat_pdqueiros.settings import BOXES_FOLDER_ARCHIVED_INPUT, FIELDS_FOLDER_ARCHIVED_INPUT, BOXES_FOLDER_INPUT, FIELDS_FOLDER_INPUT

ASSETS_DOCUMENTS = {c.__name__:c for c in [FieldDocument, BoundingBoxDocument]}

@dataclass
class AssetDataDocument():
    s3_path: str = field(default=None)
    file_name: str = field(default=None)
    archived_s3_path: str = field(default=None)
    local_input_file_path: str = field(default=None)
    local_input_folder_path: str = field(default=None)
    local_output_file_path: str = field(default=None)
    local_output_folder_path: str = field(default=None)
    document_type: str = field(default=None)
    document_class: FieldDocument | BoundingBoxDocument = field(default=None)


    def __post_init__(self):
        if self.s3_path or self.local_input_file_path or self.local_output_file_path:
            self.file_name = Path(self.s3_path or self.local_input_file_path or self.local_output_file_path).name

        if not self.local_input_folder_path and self.local_input_file_path:
            self.local_input_folder_path = Path(self.local_input_file_path).parent

        if not self.local_input_file_path and self.local_input_folder_path:
            self.local_input_file_path = os.path.join(self.local_input_folder_path, self.file_name)

        if not self.local_output_folder_path and self.local_output_file_path:
            self.local_output_folder_path = Path(self.local_output_file_path).parent

        if not self.local_output_file_path and self.local_output_folder_path:
            self.local_output_file_path = os.path.join(self.local_output_folder_path, self.file_name)

        if not self.document_type:
            if BOXES_FOLDER_INPUT in self.s3_path:
                self.document_type = BoundingBoxDocument.__name__
            elif FIELDS_FOLDER_INPUT in self.s3_path:
                self.document_type = FieldDocument.__name__
        if self.document_type:
            self.document_class = ASSETS_DOCUMENTS[self.document_type]
        if self.s3_path:
            if BOXES_FOLDER_INPUT in self.s3_path:
                self.archived_s3_path = self.s3_path.replace(BOXES_FOLDER_INPUT, BOXES_FOLDER_ARCHIVED_INPUT)
            elif FIELDS_FOLDER_INPUT in self.s3_path:
                self.archived_s3_path = self.s3_path.replace(FIELDS_FOLDER_INPUT, FIELDS_FOLDER_ARCHIVED_INPUT)
        logger.debug(f'Created {self}')

if __name__ == '__main__':
    s3_path = '/home/pedroq/workspace/airflow_hydrosat_pdqueiros/tests/data/fields/input/01978c3831bc710c9e0663456e70de1e/fields_2025-06-01_01978c3831cf71a892a1f069a16edf25.jsonl'
    doc = AssetDataDocument(s3_path=s3_path)
    print(doc)