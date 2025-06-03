import json
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

def setup_database():
    client = MongoClient("mongodb://localhost:27017")
    db = client["soubieux_giraudon_drones"]

    schema = {
        "bsonType": "object",
        "required": ["drone_id", "model", "manufacturer", "battery_capacity_mAh", "deployment_zone", "active"],
        "properties": {
            "drone_id": {"bsonType": "string"},
            "model": {"bsonType": "string"},
            "manufacturer": {"bsonType": "string"},
            "battery_capacity_mAh": {"bsonType": "int"},
            "deployment_zone": {"bsonType": "string"},
            "active": {"bsonType": "bool"}
        }
    }

    try:
        db.create_collection("drones", validator={"$jsonSchema": schema})
    except CollectionInvalid:
        print("Collection 'drones' déjà existante.")

    with open("data/drones.json", "r") as f:
        drones = json.load(f)
        db.drones.delete_many({})
        db.drones.insert_many(drones)

    print("Base de données initialisée avec succès.")

if __name__ == "__main__":
    setup_database()
