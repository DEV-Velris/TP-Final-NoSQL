from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017")
db = client["soubieux_giraudon_drones"]

def q1_temp_hourly():
    return list(db.drone_measurements.aggregate([
        {"$match": {"timestamp": {"$gte": datetime.utcnow() - timedelta(hours=12)}}},
        {"$group": {
            "_id": {"$hour": "$timestamp"},
            "avgTemp": {"$avg": "$temperature"}
        }},
        {"$sort": {"_id": 1}}
    ]))

def q2_last_measure_by_drone():
    return list(db.drone_measurements.aggregate([
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$drone_id",
            "last_measure": {"$first": "$$ROOT"}
        }}
    ]))

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

def q5_near_montsouris():
    return db.drone_measurements.count_documents({
        "location": {
            "$geoWithin": {
                "$centerSphere": [[2.3388, 48.8210], 3 / 6378.1]
            }
        }
    })

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
