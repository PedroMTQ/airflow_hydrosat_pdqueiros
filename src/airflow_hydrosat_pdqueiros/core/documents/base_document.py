from abc import abstractmethod
from typing import Optional, Any
import numpy as np
from airflow_hydrosat_pdqueiros.io.logger import logger
from pydantic import BaseModel, Field, ConfigDict, field_serializer, field_validator

class BaseDocument(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True, # Required for np.ndarray
        validate_assignment=True      # Ensures validation on attribute updates
    )
    coordinates_x_min: float = Field(repr=False)
    coordinates_y_min: float = Field(repr=False)
    coordinates_x_max: float = Field(repr=False)
    coordinates_y_max: float = Field(repr=False)
    box_id: str = Field(description='ID of this data asset')
    irrigation_array: Optional[np.ndarray] = Field(default=None, repr=False)
    irrigation_density: Optional[float] = Field(default=None,description='Irrigation density for all pixels in the assets area')
    is_processed: bool = Field(default=False, description='Processing status of this asset')

    @field_validator('irrigation_array', mode='before')
    @classmethod
    def validate_numpy_array(cls, value: Any) -> Any:
        """Converts lists to numpy arrays automatically during initialization."""
        if isinstance(value, list):
            return np.array(value)
        return value

    @abstractmethod
    def process(self):
        return

    @field_serializer('irrigation_array')
    def serialize_irrigation_array(self, value: Optional[np.ndarray]):
        if value is None:
            return None
        return value.tolist()


if __name__ == '__main__':
    base_doc = BaseDocument(0,0,2,2,3)
