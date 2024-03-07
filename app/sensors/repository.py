from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
import json

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(sensor: schemas.SensorCreate, db: Session, mongodb: MongoDBClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name) #Afegir el sensor en la base SQL
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    mydoc = { #Crear document amb les dades del sensor
        "id": db_sensor.id,
        #"longitude": sensor.longitude,
        #"latitude": sensor.latitude,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        },
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version 
    }
    mongodb.set(mydoc) #Afegir el sensor a la base mongoDB
    return db_sensor

#metode per registrar nous dades, per parametre pasem les dues bases de dades, la id del sensor i les noves dades
def record_data(db: Session,redis: RedisClient, sensor_id: int, data: schemas.SensorData, mongodb:MongoDBClient) -> schemas.Sensor:
    try: #control d'excepcions
        serialized_data = json.dumps(data.dict()) #serialitzem les dades per a que es pugui fer el set en radis
        redis.set(sensor_id,serialized_data) #cridem el metode setter per actualizar les dades 
        db_sensordata_serie = redis.get(sensor_id) #cridem el metode getter per obtenir les dades actualitzades del sensor
        db_sensordata = schemas.SensorData.parse_raw(db_sensordata_serie) #deserialitzem les dades per poder accedir a elles
        db_sensor = get_sensor(db,sensor_id) #cridem el metode per obtenir el sensor actual
        mongo_sensor=mongodb.get({"id": sensor_id})
        if mongo_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        sensor = schemas.Sensor(id = db_sensor.id, name = db_sensor.name,
                                    latitude = mongo_sensor["location"]["coordinates"][0], longitude=mongo_sensor["location"]["coordinates"][1],
                                    joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"), 
                                    last_seen=db_sensordata.last_seen, type=mongo_sensor["type"], mac_address=mongo_sensor["mac_address"],
                                    temperature=db_sensordata.temperature, 
                                    humidity=db_sensordata.humidity, battery_level=db_sensordata.battery_level,
                                    velocity=db_sensordata.velocity) #creem un nou sensor amb totes les dades
        return sensor #retornem el sensor amb les dades
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor

#metode per obtenir les dades del sensor
def get_data(db: Session,redis: RedisClient, sensor_id: int, mongodb:MongoDBClient) -> schemas.Sensor:
    try:
        db_sensordata_serie = redis.get(sensor_id) #cridem el metode getter per obtenir les dades actualitzades del sensor
        db_sensordata = schemas.SensorData.parse_raw(db_sensordata_serie) #deserialitzem les dades per poder accedir a elles
        db_sensor = get_sensor(db,sensor_id) #cridem el metode per obtenir el sensor actual
        mongo_sensor=mongodb.get({"id":sensor_id})
        if mongo_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        sensor = schemas.Sensor(id = db_sensor.id, name = db_sensor.name,
                                    latitude = mongo_sensor["location"]["coordinates"][0], longitude=mongo_sensor["location"]["coordinates"][1],
                                    joined_at=db_sensor.joined_at.strftime("%m/%d/%Y, %H:%M:%S"), 
                                    last_seen=db_sensordata.last_seen, type=mongo_sensor["type"], mac_address=mongo_sensor["mac_address"],
                                    temperature=db_sensordata.temperature, 
                                    humidity=db_sensordata.humidity, battery_level=db_sensordata.battery_level,
                                    velocity=db_sensordata.velocity) #creem un nou sensor amb totes les dades
        return sensor #retornem el sensor amb les dades
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def get_sensors_near(mongodb: MongoDBClient, latitude: float, longitude: float, radius: int, db: Session, redis: RedisClient):
    try:
        mongodb.getDatabase("sensors")
        collection = mongodb.getCollection("sensorsData")
        collection.create_index([("location","2dsphere")])
        sensors = collection.find(
                {
                "location":
                    { "$near" :
                        {
                            "$geometry":{"type": "Point",
                                        "coordinates": [ longitude, latitude ] 
                                        },
                            "$maxDistance": radius
                        }
                    }
                }
        )
        sensors = list(sensors)
        near_sensors = []
        for sensor in sensors:
            near_sensors.append(get_data(db=db, redis=redis, sensor_id=sensor["id"],mongodb=mongodb))
        if near_sensors is None:
            return []
        return near_sensors
    except:
        raise HTTPException(status_code=404, detail="Sensor not found") #excepció en cas de que no existi el sensor
