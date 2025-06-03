# Ce script écoute un topic MQTT, reçoit des mesures de drones et les insère dans une collection timeseries MongoDB.
import json
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime

# Connexion à la base MongoDB locale et sélection de la base de données
client = MongoClient("mongodb://localhost:27017")
db = client["soubieux_giraudon_drones"]

# Initialise la collection timeseries si elle n'existe pas déjà
# La collection stocke les mesures des drones avec un champ de temps et un champ méta pour l'id du drone
def init_timeseries_collection():
    if "drone_measurements" not in db.list_collection_names():
        db.create_collection(
            "drone_measurements",
            timeseries={
                "timeField": "timestamp",
                "metaField": "drone_id",
                "granularity": "seconds"
            }
        )
    print("Collection timeseries prête.")

# Callback appelé à chaque message reçu sur le topic MQTT
# Décode le message, convertit le timestamp, insère dans MongoDB
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        payload["timestamp"] = datetime.fromisoformat(payload["timestamp"])
        db.drone_measurements.insert_one(payload)
        print(f"Données insérées : {payload}")
    except Exception as e:
        print("Erreur :", e)

# Fonction principale : initialise la collection, configure le client MQTT et démarre l'écoute
def main():
    init_timeseries_collection()

    mqtt_client = mqtt.Client(protocol=mqtt.MQTTv5)
    mqtt_client.connect("broker.hivemq.com", 1883)
    mqtt_client.subscribe("autonomous_drones/sensors")
    mqtt_client.on_message = on_message
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("Arrêt du listener MQTT (Ctrl+C détecté).")

if __name__ == "__main__":
    main()
