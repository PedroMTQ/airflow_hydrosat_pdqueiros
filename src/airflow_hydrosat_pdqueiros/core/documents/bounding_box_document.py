from dataclasses import dataclass

import numpy as np

from hydrosat_pdqueiros.services.core.documents.base_document import BaseDocument


@dataclass
class BoundingBoxDocument(BaseDocument):
    def __post_init__(self):
        if self.irrigation_array is None:
            height = int(self.coordinates_y_max - self.coordinates_y_min)
            width = int(self.coordinates_x_max - self.coordinates_x_min)
            self.irrigation_array = np.random.choice([0, 1], size=(height, width))
        height = int(self.coordinates_y_max - self.coordinates_y_min)
        width = int(self.coordinates_x_max - self.coordinates_x_min)
        assert self.irrigation_array.shape == (height, width), f'Array shape {self.irrigation_array.shape} does not match expected ({height}, {width}) from coordinates'


    def is_valid(self) -> bool:
        return True



if __name__ == '__main__':
    doc = BoundingBoxDocument(coordinates_x_min=0,
                              coordinates_y_min=10,
                              coordinates_x_max=10,
                              coordinates_y_max=20,
                              box_id='hg',
                              )
    print(doc)
    doc_dict = doc.to_dict()
    print(doc_dict)
    doc = BoundingBoxDocument.from_dict(doc_dict)
    print('2', doc)
