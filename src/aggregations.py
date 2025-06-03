from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017")
db = client["soubieux_giraudon_drones"]

# Moyenne de la température par heure sur les 12 dernières heures
# $match : filtre les mesures dont le timestamp est dans les 12 dernières heures
# $group : groupe par heure du timestamp et calcule la moyenne de température
# $sort : trie par heure croissante
def q1_temp_hourly():
    return list(db.drone_measurements.aggregate([
        {"$match": {"timestamp": {"$gte": datetime.utcnow() - timedelta(hours=12)}}},
        {"$group": {
            "_id": {"$hour": "$timestamp"},
            "avgTemp": {"$avg": "$temperature"}
        }},
        {"$sort": {"_id": 1}}
    ]))

# Dernière mesure pour chaque drone
# $sort : trie les mesures par timestamp décroissant
# $group : groupe par drone_id et prend la première mesure (la plus récente)
def q2_last_measure_by_drone():
    return list(db.drone_measurements.aggregate([
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$drone_id",
            "last_measure": {"$first": "$$ROOT"}
        }}
    ]))

# Modèle des 5 dernières mesures
# $sort : trie par timestamp décroissant
# $limit : ne garde que les 5 plus récentes
# $lookup : joint avec la collection drones sur drone_id
# $unwind : déplie le tableau drone_info
# $project : ne garde que le timestamp et le modèle du drone
def q3_last5_models():
    return list(db.drone_measurements.aggregate([
        {"$sort": {"timestamp": -1}},
        {"$limit": 5},
        {"$lookup": {
            "from": "drones",
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$project": {
            "timestamp": 1,
            "model": "$drone_info.model"
        }}
    ]))

# Nombre de mesures par fabricant de drone
# $lookup : joint avec la collection drones sur drone_id
# $unwind : déplie le tableau drone_info
# $group : groupe par fabricant et compte le nombre de mesures
def q4_count_by_manufacturer():
    return list(db.drone_measurements.aggregate([
        {"$lookup": {
            "from": "drones",
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$group": {
            "_id": "$drone_info.manufacturer",
            "count": {"$sum": 1}
        }}
    ]))

# Nombre de mesures dans un rayon de 3km autour de Montsouris
# $geoWithin/$centerSphere : filtre les mesures dont la localisation est dans le cercle spécifié
def q5_near_montsouris():
    return db.drone_measurements.count_documents({
        "location": {
            "$geoWithin": {
                "$centerSphere": [[2.3388, 48.8210], 3 / 6378.1]
            }
        }
    })

# Moyenne du pm25 pour chaque drone actif
# $match : ne garde que les mesures avec pollution.pm25
# $lookup : joint avec la collection drones
# $unwind : déplie le tableau drone_info
# $match : ne garde que les drones actifs
# $group : groupe par drone et calcule la moyenne du pm25
def q6_pm25_avg_active():
    return list(db.drone_measurements.aggregate([
        {"$match": {"pollution.pm25": {"$exists": True}}},
        {"$lookup": {
            "from": "drones",
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$match": {"drone_info.active": True}},
        {"$group": {
            "_id": "$drone_id",
            "avg_pm25": {"$avg": "$pollution.pm25"}
        }}
    ]))

# Dernière mesure pour chaque drone actif
# $sort : trie par timestamp décroissant
# $lookup : joint avec la collection drones
# $unwind : déplie le tableau drone_info
# $match : ne garde que les drones actifs
# $group : groupe par drone et prend la première mesure (la plus récente)
def q7_last_per_active_drone():
    return list(db.drone_measurements.aggregate([
        {"$sort": {"timestamp": -1}},
        {"$lookup": {
            "from": "drones",
            "localField": "drone_id",
            "foreignField": "drone_id",
            "as": "drone_info"
        }},
        {"$unwind": "$drone_info"},
        {"$match": {"drone_info.active": True}},
        {"$group": {
            "_id": "$drone_id",
            "last_data": {"$first": "$$ROOT"}
        }}
    ]))
