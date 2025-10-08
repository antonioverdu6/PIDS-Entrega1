from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
from pymongo import MongoClient
from typing import Any, Text, Dict, List

# --- Configuración de conexión a Mongo ---
MONGO_URI = "mongodb://taxi-mongo:27017"
DB_NAME = "taxi_db"
COLL_NAME = "trips"

def get_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLL_NAME]


# --- Acción: calcular la distancia media ---
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


# --- Acción: encontrar la hora más frecuente ---
class ActionMostFrequentHour(Action):
    def name(self) -> Text:
        return "action_most_frequent_hour"

    def run(self, dispatcher, tracker, domain):
        coll = get_collection()

        pipeline = [
            # ✅ Filtra registros donde el campo no existe o es nulo/vacío
            {
                "$match": {
                    "tpep_pickup_datetime": {"$ne": None, "$ne": ""}
                }
            },
            {
                "$addFields": {
                    "pickup_date": {
                        "$convert": {
                            "input": "$tpep_pickup_datetime",
                            "to": "date",
                            "onError": None,     # evita fallo si no se puede convertir
                            "onNull": None
                        }
                    }
                }
            },
            {
                "$project": {
                    "hour": {"$hour": "$pickup_date"}
                }
            },
            {"$group": {"_id": "$hour", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]

        try:
            result = list(coll.aggregate(pipeline))
            if result:
                hour = int(result[0]["_id"])
                dispatcher.utter_message(text=f"The most frequent pickup hour is {hour}:00.")
            else:
                dispatcher.utter_message(text="I couldn't determine the most frequent pickup hour.")
        except Exception as e:
            dispatcher.utter_message(text=f"Error while querying MongoDB: {e}")
        return []



# --- Acción: mostrar el último viaje registrado ---
# --- Action: show the last recorded trip ---

class ActionShowLastTrip(Action):
    def name(self) -> Text:
        return "action_show_last_trip"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        last_trip = coll.find_one(sort=[("tpep_dropoff_datetime", -1)])

        if last_trip:
            pickup = last_trip.get("tpep_pickup_datetime")
            dropoff = last_trip.get("tpep_dropoff_datetime")
            distance = last_trip.get("trip_distance", 0)
            total = last_trip.get("total_amount", 0)

            message = (
                f"Last recorded trip:\n"
                f"- Start: {pickup}\n"
                f"- End: {dropoff}\n"
                f"- Distance: {distance:.2f} miles\n"
                f"- Total: ${total:.2f}"
            )
        else:
            message = "I couldn't find any trips in the database."

        dispatcher.utter_message(text=message)
        return []


class ActionAverageTripDuration(Action):
    def name(self) -> Text:
        return "action_average_trip_duration"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()

        # Convert to dates before subtracting (fix for string fields)
        pipeline = [
            {
                "$project": {
                    "duration_seconds": {
                        "$divide": [
                            {
                                "$subtract": [
                                    {"$toDate": "$tpep_dropoff_datetime"},
                                    {"$toDate": "$tpep_pickup_datetime"}
                                ]
                            },
                            1000  # convert ms to seconds
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "avg_duration": {"$avg": "$duration_seconds"}
                }
            }
        ]

        try:
            result = list(coll.aggregate(pipeline))
            if result and result[0].get("avg_duration"):
                avg_minutes = result[0]["avg_duration"] / 60
                dispatcher.utter_message(
                    text=f"The average trip duration is about {avg_minutes:.1f} minutes."
                )
            else:
                dispatcher.utter_message(
                    text="I couldn't calculate the average trip duration from the data."
                )
        except Exception as e:
            dispatcher.utter_message(
                text=f"An error occurred while calculating trip duration: {str(e)}"
            )

        return []
    

# --- Action: compute the average tip amount ---
class ActionAverageTipAmount(Action):
    def name(self) -> Text:
        return "action_average_tip_amount"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        pipeline = [
            {"$group": {"_id": None, "avg_tip": {"$avg": "$tip_amount"}}}
        ]

        result = list(coll.aggregate(pipeline))
        if result and result[0].get("avg_tip") is not None:
            avg_tip = result[0]["avg_tip"]
            dispatcher.utter_message(
                text=f"The average tip amount is ${avg_tip:.2f}."
            )
        else:
            dispatcher.utter_message(
                text="I couldn't calculate the average tip amount from the data."
            )

        return []

# --- Action: calculate the total revenue collected ---
class ActionTotalRevenue(Action):
    def name(self) -> Text:
        return "action_total_revenue"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        pipeline = [
            {"$group": {"_id": None, "total_revenue": {"$sum": "$total_amount"}}}
        ]

        result = list(coll.aggregate(pipeline))
        if result and result[0].get("total_revenue") is not None:
            total_revenue = result[0]["total_revenue"]
            dispatcher.utter_message(
                text=f"The total revenue collected from all trips is ${total_revenue:,.2f}."
            )
        else:
            dispatcher.utter_message(
                text="I couldn't calculate the total revenue from the data."
            )

        return []


# --- Action: count the total number of trips ---
class ActionTotalNumberOfTrips(Action):
    def name(self) -> Text:
        return "action_total_number_of_trips"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        coll = get_collection()
        total_trips = coll.count_documents({})

        if total_trips > 0:
            dispatcher.utter_message(
                text=f"There are {total_trips:,} trips recorded in the database."
            )
        else:
            dispatcher.utter_message(
                text="I couldn't find any trips in the database."
            )

        return []
