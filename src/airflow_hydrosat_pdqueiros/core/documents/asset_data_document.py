import os
from dataclasses import dataclass, field
from pathlib import Path

from hydrosat_pdqueiros.services.core.documents.bounding_box_document import BoundingBoxDocument
from hydrosat_pdqueiros.services.core.documents.field_document import FieldDocument
from hydrosat_pdqueiros.services.io.logger import logger

ASSETS_DOCUMENTS = {c.__name__:c for c in [FieldDocument, BoundingBoxDocument]}

@dataclass
class AssetDataDocument():
    s3_path: str = field(default=None)
    file_name: str = field(default=None)
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

        if self.document_type:
            self.document_class = ASSETS_DOCUMENTS[self.document_type]

        logger.debug(f'Created {self}')
