import pytest
from pymongo import MongoClient

@pytest.fixture(scope="function")
def init_test_data():
    client = MongoClient("mongodb://localhost:27017/")
    db = client['accident_test_db']
    car_accidents = db['car_accidents']

    yield car_accidents
    car_accidents.drop()
    client.close()
