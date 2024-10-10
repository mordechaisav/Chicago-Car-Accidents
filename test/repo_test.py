import pytest
from pymongo.collection import Collection
from returns.result import Success

from repository.csv_repository import *
from repository.crash_reposintory import *



@pytest.fixture(scope="function")
def collections(init_crash_data(car_accidents,)):
    return init_test_data['accidents']



def test_get_by_area(collections: Collection):
    res = count_accidents_by_area("225")
    assert isinstance(res, Success)
    assert len(res.unwrap()) > 0


def test_get_by_period(collections: Collection):
    res = count_accidents_by_time_and_area("225","month",datetime(2022,1,5))
    assert isinstance(res, Success)
    assert res.unwrap() > 0

def test_get_by_cause_area(collections: Collection):
    res = get_accidents_grouped_by_cause("225")
    assert isinstance(res, Success)
    assert res.unwrap() > 0

def test_get_accidents_by_injured_area(collections: Collection):
    res = get_accidents_statistics("225")
    assert isinstance(res, Success)
    assert res.unwrap() > 0