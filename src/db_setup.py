# db_setup.py : Script d'initialisation de la base de données MongoDB pour les drones
import json
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid

# Fonction principale pour configurer la base de données
# Connexion à MongoDB, création de la collection avec schéma, et insertion des données

def setup_database():
    # Connexion au serveur MongoDB local
    client = MongoClient("mongodb://localhost:27017")
    db = client["soubieux_giraudon_drones"]

    # Définition du schéma de validation pour la collection 'drones'
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
        # Création de la collection avec validation du schéma
        db.create_collection("drones", validator={"$jsonSchema": schema})
    except CollectionInvalid:
        # Si la collection existe déjà, on affiche un message
        print("Collection 'drones' déjà existante.")

    # Chargement des données de drones depuis le fichier JSON
    with open("data/drones.json", "r") as f:
        drones = json.load(f)
        # Suppression des documents existants pour éviter les doublons
        db.drones.delete_many({})
        # Insertion des nouveaux documents
        db.drones.insert_many(drones)

    print("Base de données initialisée avec succès.")

# Point d'entrée du script
if __name__ == "__main__":
    setup_database()
