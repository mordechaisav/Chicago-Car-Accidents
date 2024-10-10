from database.connect import car_accidents
from repository.crash_reposintory import *

def create_index():
    car_accidents.create_index([('location.beat_of_occurrence', 1)])
    car_accidents.create_index([('crash_date.date', 1)])
    car_accidents.create_index([('prim_contributory_cause', 1)])
    car_accidents.create_index([('location.beat_of_occurrence', 1), ('crash_date.date', 1)])
    car_accidents.create_index([('injuries.total', 1), ('injuries.fatal', 1)])
    car_accidents.create_index([('num_units', 1), ('location.beat_of_occurrence', 1)])

def initialize_index():
    start_date = datetime.now()
    count_accidents_by_area("225")
    end_date = datetime.now()
    total = end_date-start_date
    create_index()
    start_date = datetime.now()
    count_accidents_by_area("225")
    end_date = datetime.now()
    tota_after_index = end_date-start_date
    return total-tota_after_index


