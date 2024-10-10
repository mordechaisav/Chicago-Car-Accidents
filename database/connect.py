from pymongo import MongoClient


client = MongoClient('mongodb://localhost:27017/')
accidents_db = client['chicago-car-accidents']


car_accidents = accidents_db['car_accidents']
locations = accidents_db['locations']
