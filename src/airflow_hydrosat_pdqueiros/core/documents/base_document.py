from dataclasses import dataclass, field

import numpy as np
from airflow_hydrosat_pdqueiros.io.logger import logger


@dataclass
class BaseDocument():
    coordinates_x_min: float = field(repr=False)
    coordinates_y_min: float = field(repr=False)
    coordinates_x_max: float = field(repr=False)
    coordinates_y_max: float = field(repr=False)
    box_id : str
    irrigation_array: np.array = field(default=None)
    is_processed: np.array = field(default=False)

    @classmethod
    def from_dict(cls, data: dict):
        if isinstance(data.get('irrigation_array'), list):
            data['irrigation_array'] = np.array(data['irrigation_array'])
        try:
            return cls(**data)
        except Exception as e:
            logger.exception(e)
            return None


    # fake processing
    def process(self):
        self.is_processed = True

    def to_dict(self):
        return {
                'box_id': self.box_id,
                'coordinates_x_min': self.coordinates_x_min,
                'coordinates_y_min': self.coordinates_y_min,
                'coordinates_x_max': self.coordinates_x_max,
                'coordinates_y_max': self.coordinates_y_max,
                'irrigation_array': self.irrigation_array.tolist(),
                'is_processed': self.is_processed,
        }

if __name__ == '__main__':
    base_doc = BaseDocument(0,0,2,2,3)
