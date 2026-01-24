import numpy as np
from airflow_hydrosat_pdqueiros.core.documents.base_document import BaseDocument
from airflow_hydrosat_pdqueiros.core.documents.bounding_box_document import BoundingBoxDocument
from shapely.geometry import box
from pydantic import model_validator
from airflow_hydrosat_pdqueiros.io.logger import logger


class FieldDocument(BaseDocument):

    @model_validator(mode='after')
    def initialize_field_array(self) -> 'FieldDocument':
        if self.irrigation_array is None:
            height = int(self.coordinates_y_max - self.coordinates_y_min)
            width = int(self.coordinates_x_max - self.coordinates_x_min)
            self.irrigation_array = np.zeros((height, width))
        return self

    def process(self):
        return

    def irrigate2(self, bounding_box_document: BoundingBoxDocument):
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
            logger.debug(f'No intersection between field {field_box} and bounding box {bounding_box}')
            return
        # non-sensical but does the job for now
        height = int(self.coordinates_y_max - self.coordinates_y_min)
        width = int(self.coordinates_x_max - self.coordinates_x_min)
        logger.debug(f'Field before irrigation: {self.irrigation_array}')
        self.irrigation_array = np.random.choice([0, 1], size=(height, width))
        logger.debug(f'Field after irrigation: {self.irrigation_array}')

    def irrigate(self, bounding_box_document: BoundingBoxDocument):
        """
        Overlays the bounding box irrigation data onto the field array.
        Maps global spatial coordinates to local array indices to update pixels.
        """
        # 1. Determine the spatial bounds of the intersection
        # We take the max of the mins and the min of the maxes
        inter_x_min = max(self.coordinates_x_min, bounding_box_document.coordinates_x_min)
        inter_y_min = max(self.coordinates_y_min, bounding_box_document.coordinates_y_min)
        inter_x_max = min(self.coordinates_x_max, bounding_box_document.coordinates_x_max)
        inter_y_max = min(self.coordinates_y_max, bounding_box_document.coordinates_y_max)

        # 2. Check if there is a valid intersection area
        if inter_x_min >= inter_x_max or inter_y_min >= inter_y_max:
            logger.debug(f'No spatial intersection for field {self.box_id}')
            return

        # 3. Map global coordinates to Field Array indices
        # Index = Global Coord - Global Origin
        f_y_start = int(inter_y_min - self.coordinates_y_min)
        f_y_end = int(inter_y_max - self.coordinates_y_min)
        f_x_start = int(inter_x_min - self.coordinates_x_min)
        f_x_end = int(inter_x_max - self.coordinates_x_min)

        # 4. Map global coordinates to BoundingBox Array indices
        b_y_start = int(inter_y_min - bounding_box_document.coordinates_y_min)
        b_y_end = int(inter_y_max - bounding_box_document.coordinates_y_min)
        b_x_start = int(inter_x_min - bounding_box_document.coordinates_x_min)
        b_x_end = int(inter_x_max - bounding_box_document.coordinates_x_min)

        # 5. Extraction and Logic Update
        # We extract the overlapping slice from the bounding box
        bbox_slice = bounding_box_document.irrigation_array[b_y_start:b_y_end, b_x_start:b_x_end]

        logger.debug(f'Field before irrigation: {self.irrigation_array}')
        # Use NumPy's logical OR (|=) to turn 0s into 1s where the bbox has 1s
        # This preserves existing 1s in the field (the "irrigate" logic)
        self.irrigation_array[f_y_start:f_y_end, f_x_start:f_x_end] = np.logical_or(
            self.irrigation_array[f_y_start:f_y_end, f_x_start:f_x_end],
            bbox_slice
        ).astype(int)
        logger.debug(f'Field after irrigation: {self.irrigation_array}')
        logger.debug(f'Irrigated field {self.box_id} using bbox {bounding_box_document.box_id}')


def test_irrigate_partial_overlap():
    # 1. Setup a Field (10x10) at global origin (0,0)
    # Field indices: [0-9, 0-9]
    field = FieldDocument(
        coordinates_x_min=0.0,
        coordinates_y_min=0.0,
        coordinates_x_max=10.0,
        coordinates_y_max=10.0,
        box_id='test_field'
    )
    # Ensure it starts at zero
    field.irrigation_array = np.zeros((10, 10), dtype=int)

    # 2. Setup a BoundingBox (5x5) that partially overlaps
    # Overlap region: X[5-10], Y[5-10]
    # This bbox starts at (5,5) and goes to (10,10)
    bbox_array = np.ones((5, 5), dtype=int)
    bbox = BoundingBoxDocument(
            coordinates_x_min=7.0,
            coordinates_y_min=7.0,
            coordinates_x_max=12.0, # 12 - 7 = 5 (Matches width)
            coordinates_y_max=12.0, # 12 - 7 = 5 (Matches height)
            box_id='bbox_partial',
            irrigation_array=bbox_array
        )

    # 3. Execute Irrigation
    field.irrigate(bbox)
    # 4. Assertions
    # The field is 10x10. The bbox starts at 7. 
    # Therefore, indices 7, 8, and 9 should be 1.
    overlap_slice = field.irrigation_array[7:10, 7:10]
    assert overlap_slice.shape == (3, 3), "Overlap area should be 3x3"
    assert np.all(overlap_slice == 1), "Overlapping pixels should be irrigated"
    # Check a pixel that should remain dry (outside the bbox range)
    assert field.irrigation_array[0, 0] == 0, "Pixel at (0,0) should remain 0"
    print("Partial overlap test passed.")

def test_irrigate_no_overlap():
    field = FieldDocument(0, 0, 2, 2, 'f1')
    bbox = BoundingBoxDocument(5, 5, 7, 7, 'b1') # Completely disjoint
    initial_sum = np.sum(field.irrigation_array)
    field.irrigate(bbox)
    assert np.sum(field.irrigation_array) == initial_sum, "Field should not change if no overlap"

if __name__ == '__main__':
    test_irrigate_partial_overlap()
