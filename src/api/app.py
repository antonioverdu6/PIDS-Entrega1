import os
from datetime import datetime
from bson import ObjectId
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://taxi-mongo:27017/taxi_db")

app = Flask(__name__)
CORS(app)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
db = client.get_default_database()   # taxi_db
trips = db["trips"]

def serialize(doc):
    if not doc:
        return doc
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out

@app.route("/health", methods=["GET"])
def health():
    try:
        client.admin.command("ping")
        return jsonify(status="ok"), 200
    except ServerSelectionTimeoutError as e:
        return jsonify(status="error", detail=str(e)), 500

@app.route("/trips/count", methods=["GET"])
def trips_count():
    return jsonify(count=trips.count_documents({}))

@app.route("/trips/sample", methods=["GET"])
def trips_sample():
    n = int(request.args.get("n", 5))
    docs = list(trips.aggregate([{"$sample": {"size": n}}]))
    return jsonify([serialize(d) for d in docs])

@app.route("/trips/by-hour", methods=["GET"])
def trips_by_hour():
    pipeline = [
        {"$match": {"tpep_pickup_datetime": {"$type": "date"}}},
        {"$group": {"_id": {"$hour": "$tpep_pickup_datetime"}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    res = list(trips.aggregate(pipeline))
    return jsonify(res)

@app.route("/trips/stats", methods=["GET"])
def trips_stats():
    # filtros opcionales: pulocationid, dolocationid, payment_type, date_from, date_to
    q = {}
    pul = request.args.get("pulocationid")
    dol = request.args.get("dolocationid")
    pay = request.args.get("payment_type")
    date_from = request.args.get("date_from")  # ISO: 2020-01-01
    date_to = request.args.get("date_to")

    if pul: q["PULocationID"] = int(pul)
    if dol: q["DOLocationID"] = int(dol)
    if pay: q["payment_type"] = int(pay)

    if date_from or date_to:
        q["tpep_pickup_datetime"] = {}
        if date_from:
            q["tpep_pickup_datetime"]["$gte"] = datetime.fromisoformat(date_from)
        if date_to:
            q["tpep_pickup_datetime"]["$lte"] = datetime.fromisoformat(date_to)

    pipeline = [
        {"$match": q},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "avg_fare": {"$avg": "$fare_amount"},
            "avg_tip": {"$avg": "$tip_amount"},
            "avg_distance": {"$avg": "$trip_distance"},
            "max_distance": {"$max": "$trip_distance"}
        }}
    ]
    res = list(trips.aggregate(pipeline))
    return jsonify(res[0] if res else {"count": 0})

@app.route("/trips", methods=["GET"])
def trips_list():
    # paginación + filtros simples
    limit = min(int(request.args.get("limit", 20)), 200)
    page = max(int(request.args.get("page", 1)), 1)
    skip = (page - 1) * limit

    q = {}
    for key in ("PULocationID", "DOLocationID", "payment_type"):
        if key.lower() in request.args:
            q[key] = int(request.args[key.lower()])

    cursor = trips.find(q).skip(skip).limit(limit)
    docs = [serialize(d) for d in cursor]
    total = trips.count_documents(q)
    return jsonify({
        "page": page,
        "limit": limit,
        "total": total,
        "items": docs
    })

if __name__ == "__main__":
    # Desarrollo local (no en contenedor)
    app.run(host="0.0.0.0", port=8000, debug=True)
