from dataclasses import dataclass

import numpy as np
from shapely.geometry import box

from hydrosat_pdqueiros.services.core.documents.base_document import BaseDocument
from hydrosat_pdqueiros.services.core.documents.bounding_box_document import BoundingBoxDocument


@dataclass
class FieldDocument(BaseDocument):

    def __post_init__(self):
        if self.irrigation_array is None:
            height = self.coordinates_y_max - self.coordinates_y_min
            width = self.coordinates_x_max - self.coordinates_x_min
            self.irrigation_array = np.zeros((height, width))

    def is_valid(self) -> bool:
        return True

    def irrigate(self, bounding_box_document: BoundingBoxDocument):
        bounding_box = box(minx=bounding_box_document.coordinates_x_min,
                           miny=bounding_box_document.coordinates_y_min,
                           maxx=bounding_box_document.coordinates_x_max,
                           maxy=bounding_box_document.coordinates_y_max,
                            )
        field_box = box(minx=self.coordinates_x_min,
                        miny=self.coordinates_y_min,
                        maxx=self.coordinates_x_max,
                        maxy=self.coordinates_y_max,
                         )
        if not bounding_box.intersects(field_box):
            return
        # non-sensical but does the job for now
        height = int(self.coordinates_x_max- self.coordinates_y_min)
        width = int(self.coordinates_x_max - self.coordinates_x_min)
        self.irrigation_array = np.random.choice([0, 1], size=(height, width))


if __name__ == '__main__':
    field_1 = FieldDocument(0,0,2,2,2)
    bounding_box = BoundingBoxDocument(0,0,5,5, 1)
    print(field_1)
    print(field_1.irrigation_array)
    print(field_1.irrigation_array.any())
    print(field_1.irrigate(bounding_box))
    print(field_1.irrigation_array)
    print(field_1.irrigation_array.any())
