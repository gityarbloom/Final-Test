from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from uuid import UUID



class EventModel(BaseModel):
    timestamp: datetime
    id_signal: UUID
    id_entity: str
    lat_reported: float
    lon_reported: float
    type_signal: str
    priority_level: int