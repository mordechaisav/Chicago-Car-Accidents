import pytest
from pymongo.collection import Collection
from datetime import datetime
from repository.crash_reposintory import count_accidents_by_area,count_accidents_by_time_and_area,get_accidents_statistics,get_accidents_grouped_by_cause



@pytest.fixture(scope="function")
def collections(init_test_data):
    return init_test_data



def test_get_by_area(collections: Collection):
    res = count_accidents_by_area(collections,"225")
    assert res == 268



def test_get_by_period(collections: Collection):
    res = count_accidents_by_time_and_area(collections,"225","month",datetime(2022,3,5))
    assert res == 4

def test_get_by_cause_area(collections: Collection):
    res = get_accidents_grouped_by_cause(collections,"225")
    assert res is not None

def test_get_accidents_by_injured_area(collections: Collection):
    res = get_accidents_statistics(collections,"225")
    assert res is not None