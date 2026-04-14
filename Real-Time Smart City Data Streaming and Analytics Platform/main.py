# import json
# import os
# import random
# from confluent_kafka import SerializingProducer
# import simplejson
# import uuid
# from datetime import datetime, timedelta
# import time

# LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
# BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# # Calculate movement increments
# LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
# LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

# # Environment Variables for configuration
# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
# VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
# GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
# TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
# WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
# EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')


# start_time = datetime.now()
# start_location = LONDON_COORDINATES.copy()

# def get_next_time():
#     """
#     Returns the current time in the format YYYY-MM-DDTHH:MM:SS
#     """
#     global start_time
#     start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
#     return start_time

# def generate_weather_data(vehicle_id, timestamp, location):
#     return {
#         'id': uuid.uuid4(),
#         'vehicle_id': vehicle_id,
#         'location': location,
#         'timestamp': timestamp,
#         'temperature': random.uniform(-5, 26),
#         'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
#         'precipitation': random.uniform(0, 25),
#         'windSpeed': random.uniform(0, 100),
#         'humidity': random.randint(0, 100),  # percentage
#         'airQualityIndex': random.uniform(0, 500)  # AQL Value goes here
#     }

# def generate_emergency_incident_data(vehicle_id, timestamp, location):
#     return {
#         'id': uuid.uuid4(),
#         'vehicle_id': vehicle_id,
#         'incidentId': uuid.uuid4(),
#         'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
#         'timestamp': timestamp,
#         'location': location,
#         'status': random.choice(['Active', 'Resolved']),
#         'description': 'Description of the incident'
#     }

# def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
#     return {
#         'id': uuid.uuid4(),
#         'vehicle_id': vehicle_id,
#         'timestamp': timestamp,
#         'speed': random.uniform(0, 40),  # km/h
#         'direction': 'North-East',
#         'vehicleType': vehicle_type
#         }


# def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
#     return{
#         'id': uuid.uuid4(),
#         'vehicle_id': vehicle_id,
#         'camera_id': camera_id,
#         'location': location,
#         'timestamp': timestamp,
#         'snapshot': 'Base64EncodedString'
#     }


# def simulate_vehicle_movement():
#     """
#     Simulates the movement of a vehicle towards Birmingham by 
#     updating the latitude and longitude coordinates based on pre-calculated increments. 
#     Adds randomness to simulate road travel.
#     """
#     global start_location

#     #move towards birmingham
#     start_location['latitude'] += LATITUDE_INCREMENT
#     start_location['longitude'] += LONGITUDE_INCREMENT

#     #add some randomness to simulate actual road travel
#     start_location['latitude'] += random.uniform(-0.0005, 0.0005)
#     start_location['longitude'] += random.uniform(-0.0005, 0.0005)

#     return start_location


# def generate_vehicle_data(vehicle_id):
#     """
#     Generates vehicle data based on the provided 
#     vehicle_id and updates the vehicle_id by simulating vehicle movement.
#     """
#     location = simulate_vehicle_movement()
#     return {
#         'id': uuid.uuid4(),
#         'vehicle_id': vehicle_id,
#         'timestamp': get_next_time().isoformat(),
#         'location': (location['latitude'], location['longitude']),
#         'speed': random.uniform(10, 40),
#         'direction': 'North-East',
#         'make': 'Tesla',
#         'model': 'Model S',
#         'year': 2024,
#         'fuelType': 'Electric'
#     }


# def json_serializer(obj):
#     if isinstance(obj, uuid.UUID):
#         return str(obj)
#     raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Message delivery failed: {err}')
#     else:
#         print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# def produce_data_to_kafka(producer, topic, data):
#     producer.produce(
#         topic,
#         key=str(data['id']),
#         value=json.dumps(data, default=json_serializer).encode('utf-8'),
#         on_delivery=delivery_report
#     )

#     producer.flush()

# def simulate_journey(producer, vehicle_id):
#     while True:
#         vehicle_data = generate_vehicle_data(vehicle_id)
#         gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
#         traffic_camera_data = generate_traffic_camera_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'],'Intelligent AI-powered Camera')
#         weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
#         emergency_incident_data = generate_emergency_incident_data(vehicle_id, vehicle_data['timestamp'],
#                                                                    vehicle_data['location'])
        
#         if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
#                 and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
#             print('Vehicle has reached Birmingham. Simulation ending...')
#             break

#         produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
#         produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
#         produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
#         produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
#         produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

#         time.sleep(5)


# if __name__ == '__main__':
#     producer_config = {
#         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#         'error_cb': lambda err: print(f'Kafka Error: {err}')
#     }

#     producer = SerializingProducer(producer_config)

#     try:
#         simulate_journey(producer, 'Vehicle-Project-111')

#     except KeyboardInterrupt:
#         print('Simulation ended by the user.')
#     except Exception as e:
#         print(f'An unexpected error occurred: {e}')


#----------------------------------------------------- My version-----------------------------------------------#

import json
import os
import random
from confluent_kafka import SerializingProducer
import uuid
from datetime import datetime, timedelta
import time

# ----------------------------
# ROUTE: DELHI -> MUMBAI (INDIA)
# ----------------------------
DELHI_COORDINATES = {"latitude": 28.6139, "longitude": 77.2090}
MUMBAI_COORDINATES = {"latitude": 19.0760, "longitude": 72.8777}

# Movement increments (100 steps approx). Adjust divisor to change journey length.
LATITUDE_INCREMENT = (MUMBAI_COORDINATES['latitude'] - DELHI_COORDINATES['latitude']) / 100.0
LONGITUDE_INCREMENT = (MUMBAI_COORDINATES['longitude'] - DELHI_COORDINATES['longitude']) / 100.0

# ----------------------------
# ENV + TOPICS
# ----------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

# ----------------------------
# GLOBAL STATE
# ----------------------------
start_time = datetime.now()
start_location = DELHI_COORDINATES.copy()

# ----------------------------
# HELPERS
# ----------------------------
def get_next_time():
    """Returns a simulated next timestamp (ISO-8601) with 30–60s step."""
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def simulate_vehicle_movement():
    """
    Move roughly SW from Delhi to Mumbai with small randomness to mimic roads (e.g., NH48).
    """
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # Add jitter to emulate road curvature/traffic diversions
    start_location['latitude'] += random.uniform(-0.0006, 0.0006)
    start_location['longitude'] += random.uniform(-0.0006, 0.0006)

    return start_location

# ----------------------------
# DATA GENERATORS (India-tuned)
# ----------------------------
INDIAN_WEATHER = ['Sunny', 'Cloudy', 'Rain', 'Monsoon', 'Fog', 'Haze', 'Thunderstorm', 'Heatwave']
INDIAN_EMERGENCIES = [
    'Accident', 'Fire', 'Medical', 'Police', 'Breakdown',
    'Flood', 'Landslide', 'None'
]
INDIAN_VEHICLE_MAKES = [
    ('Tata', 'Nexon EV'), ('Mahindra', 'XUV400'), ('Maruti', 'Baleno'),
    ('Hyundai', 'Creta'), ('Kia', 'Seltos'), ('Tesla', 'Model 3')
]

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'location': {'lat': location['latitude'], 'lon': location['longitude']},
        'timestamp': timestamp.isoformat(),
        'temperature': round(random.uniform(15, 44), 1),           # °C
        'weatherCondition': random.choice(INDIAN_WEATHER),
        'precipitation': round(random.uniform(0, 60), 1),          # mm
        'windSpeed': round(random.uniform(0, 60), 1),              # km/h
        'humidity': random.randint(20, 100),                       # %
        'airQualityIndex': round(random.uniform(20, 350), 1)       # AQI (approx)
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(INDIAN_EMERGENCIES),
        'timestamp': timestamp.isoformat(),
        'location': {'lat': location['latitude'], 'lon': location['longitude']},
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Incident reported on NH48 near toll plaza'
    }

def generate_gps_data(vehicle_id, timestamp, vehicle_type='private'):
    # Delhi->Mumbai is broadly South-West
    direction = random.choice(['South-West', 'South', 'West', 'South-South-West'])
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp.isoformat(),
        'speed': round(random.uniform(0, 90), 1),   # km/h
        'direction': direction,
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'camera_id': camera_id,
        'location': {'lat': location['latitude'], 'lon': location['longitude']},
        'timestamp': timestamp.isoformat(),
        'snapshot': 'Base64EncodedString'  # placeholder
    }

def generate_vehicle_data(vehicle_id):
    """
    Generate vehicle telemetry & metadata and advance position along the route.
    """
    location = simulate_vehicle_movement()
    make, model = random.choice(INDIAN_VEHICLE_MAKES)
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': round(random.uniform(30, 90), 1),  # km/h cruising
        'direction': 'South-West',
        'make': make,
        'model': model,
        'year': random.choice([2021, 2022, 2023, 2024]),
        'fuelType': random.choice(['Petrol', 'Diesel', 'CNG', 'Electric', 'Hybrid'])
    }

# ----------------------------
# KAFKA PRODUCE
# ----------------------------
def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()

def reached_mumbai(curr_lat, curr_lon):
    """
    Stop when we reach/past Mumbai (both lat & lon shrink vs Delhi on this route).
    """
    return (curr_lat <= MUMBAI_COORDINATES['latitude']
            and curr_lon <= MUMBAI_COORDINATES['longitude'])

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        # Convert tuple to dict form for consistency in other events
        curr_loc = {'latitude': vehicle_data['location'][0], 'longitude': vehicle_data['location'][1]}
        ts = datetime.fromisoformat(vehicle_data['timestamp'])

        gps_data = generate_gps_data(vehicle_id, ts)
        traffic_camera_data = generate_traffic_camera_data(
            vehicle_id, ts, curr_loc, 'AI Camera - NH48'
        )
        weather_data = generate_weather_data(vehicle_id, ts, curr_loc)
        emergency_incident_data = generate_emergency_incident_data(vehicle_id, ts, curr_loc)
        # print(vehicle_data)
        # print("\n")
        # print(gps_data)
        # print("\n")
        # print(traffic_camera_data)
        # print("\n")
        # print(weather_data)
        # print("\n")
        # print(emergency_incident_data)
        # break

        # Stop condition: reached Mumbai vicinity
        if reached_mumbai(curr_loc['latitude'], curr_loc['longitude']):
            print('🚗 Vehicle has reached Mumbai. Simulation ending...')
            break

        # Produce to Kafka topics
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        time.sleep(5)

# ----------------------------
# MAIN
# ----------------------------
if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
        # Note: We're manually encoding JSON to bytes; no value.serializer needed.
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-IN-NH48-001')
    except KeyboardInterrupt:
        print('Simulation ended by the user.')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
