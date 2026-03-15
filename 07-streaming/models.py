import json
from dataclasses import dataclass
import math

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float
    lpep_pickup_datetime: str  
    lpep_dropoff_datetime: str 



def ride_from_row(row):
    passenger_count = row['passenger_count']

    if passenger_count is None or (isinstance(passenger_count, float) and math.isnan(passenger_count)):
        passenger_count = 0
    else:
        passenger_count = int(passenger_count)
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=passenger_count,
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime'])
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
