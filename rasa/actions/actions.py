from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from pymongo import MongoClient
from typing import Any, Text, Dict, List

MONGO_URI = "mongodb://taxi-mongo:27017"
DB_NAME = "taxi_db"
COLL_NAME = "trips"

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]

class ActionAverageTripDistance(Action):
    def name(self) -> Text:
        return "action_average_trip_distance"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        pipeline = [{"$group": {"_id": None, "avg_distance": {"$avg": "$trip_distance"}}}]
        result = list(coll.aggregate(pipeline))

        if result:
            avg_distance = round(result[0]["avg_distance"], 2)
            dispatcher.utter_message(text=f"The average trip distance is {avg_distance} miles.")
        else:
            dispatcher.utter_message(text="I couldn't calculate the average trip distance.")
        return []

class ActionMostFrequentHour(Action):
    def name(self) -> Text:
        return "action_most_frequent_hour"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        pipeline = [
            {"$project": {"hour": {"$hour": "$tpep_pickup_datetime"}}},
            {"$group": {"_id": "$hour", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        result = list(coll.aggregate(pipeline))

        if result:
            hour = result[0]["_id"]
            dispatcher.utter_message(text=f"The most frequent pickup hour is {hour}:00.")
        else:
            dispatcher.utter_message(text="I couldn't find the most frequent pickup hour.")
        return []

class ActionMostrarUltimoViaje(Action):
    def name(self) -> Text:
        return "action_mostrar_ultimo_viaje"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        ultimo = trips.find_one(sort=[("tpep_dropoff_datetime", -1)])

        if ultimo:
            pickup = ultimo.get("tpep_pickup_datetime")
            dropoff = ultimo.get("tpep_dropoff_datetime")
            distancia = ultimo.get("trip_distance", 0)
            total = ultimo.get("total_amount", 0)

            mensaje = (
                f" Último viaje registrado:\n"
                f"- Inicio: {pickup.strftime('%Y-%m-%d %H:%M:%S') if pickup else 'N/D'}\n"
                f"- Fin: {dropoff.strftime('%Y-%m-%d %H:%M:%S') if dropoff else 'N/D'}\n"
                f"- Distancia: {distancia:.2f} millas\n"
                f"- Total: ${total:.2f}"
            )
        else:
            mensaje = "No encontré viajes registrados en la base de datos."

        dispatcher.utter_message(text=mensaje)
        return []