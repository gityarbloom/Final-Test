from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from uuid import UUID



class EventModel(BaseModel):
    timestamp: datetime
    signal_id: UUID
    entity_id: str
    lat_reported: float
    lon_reported: float
    type_signal: str
    priority_level: int