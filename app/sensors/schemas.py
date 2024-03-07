from pydantic import BaseModel
from typing import Optional

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float #MongoDB
    longitude: float #MongoDB
    joined_at: str
    last_seen: str
    type: str #MongoDB
    mac_address: str #MongoDB
    battery_level: float
    temperature: Optional[float]
    humidity: Optional[float]
    velocity: Optional[float]
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str

class SensorData(BaseModel):
    velocity: Optional[float]
    temperature: Optional[float]
    humidity: Optional[float]
    battery_level: float
    last_seen: str