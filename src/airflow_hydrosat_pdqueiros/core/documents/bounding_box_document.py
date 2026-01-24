import numpy as np
from airflow_hydrosat_pdqueiros.core.documents.base_document import BaseDocument
from pydantic import model_validator


class BoundingBoxDocument(BaseDocument):
    def process(self):
        """
        Calculates the ratio of irrigated pixels (value 1) to total pixels.
        """
        total_pixels = self.irrigation_array.size
        irrigated_pixels = np.sum(self.irrigation_array == 1)
        self.irrigation_density = float(irrigated_pixels / total_pixels)
        self.is_processed = True

    @model_validator(mode='after')
    def validate_bounding_box(self) -> 'BoundingBoxDocument':
        """
        Internal Pydantic validator that runs automatically.
        Ensures the box has area and coordinates are logical.
        """
        if self.coordinates_x_max <= self.coordinates_x_min:
            raise ValueError(f"x_max ({self.coordinates_x_max}) must be greater than x_min ({self.coordinates_x_min})")
        if self.coordinates_y_max <= self.coordinates_y_min:
            raise ValueError(f"y_max ({self.coordinates_y_max}) must be greater than y_min ({self.coordinates_y_min})")
        if self.irrigation_array is None:
            height = int(self.coordinates_y_max - self.coordinates_y_min)
            width = int(self.coordinates_x_max - self.coordinates_x_min)
            self.irrigation_array = np.random.choice([0, 1], size=(height, width))
        height = int(self.coordinates_y_max - self.coordinates_y_min)
        width = int(self.coordinates_x_max - self.coordinates_x_min)
        assert self.irrigation_array.shape == (height, width), f'Array shape {self.irrigation_array.shape} does not match expected ({height}, {width}) from coordinates'
        return self

if __name__ == '__main__':
    doc = BoundingBoxDocument(coordinates_x_min=0,
                              coordinates_y_min=10,
                              coordinates_x_max=10,
                              coordinates_y_max=20,
                              box_id='hg',
                              )
    print(doc)
    doc_dict = doc.model_dump()
    print(doc_dict)
    doc = BoundingBoxDocument(**doc_dict)
    doc.process()
    print('2', doc)
