import pytest
from pymongo import MongoClient
from repository.csv_repository import init_crash_data
CSV_PATH = "../data/Traffic_Crashes.csv"
@pytest.fixture(scope="function")
def init_test_data():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['accident_test_db']
    car_accidents_test = db['car_accidents_test']
    init_crash_data(car_accidents_test,CSV_PATH)
    yield car_accidents_test
    car_accidents_test.drop()
    client.close()
