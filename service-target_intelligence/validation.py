from pydantic import BaseModel, Field, ValidationError
from datetime import datetime
from uuid import UUID



class EventModel(BaseModel):
    timestamp: datetime
    signal_id: UUID
    entity_id: str
    reported_lat: float
    reported_lon: float
    signal_type: str
    priority_level: int