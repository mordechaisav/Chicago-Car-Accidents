from database.connect import car_accidents,locations
from datetime import datetime, timedelta


def count_accidents_by_area(beat_of_occurrence):
    beat_of_occurrence = str(beat_of_occurrence)
    pipeline = [
        {
            '$match': {
                'location.beat_of_occurrence': beat_of_occurrence,
            }
        },
        {
            '$count': 'total_accidents'
        }
    ]
    result = list(car_accidents.aggregate(pipeline))
    if result:
        return result[0]['total_accidents']




def count_accidents_by_time_and_area(beat_of_occurrence, time_period, date):
    if time_period == 'day':
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
    elif time_period == 'week':
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=7)
    elif time_period == 'month':
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = (start_date + timedelta(days=31))
    else:
        raise ValueError("Invalid time period.")

    pipeline = [
        {


            '$match': {
                'location.beat_of_occurrence': beat_of_occurrence,
                'crash_date.date': {'$gte': start_date, '$lt': end_date}
            }
        },
        {
            '$count': 'total_accidents'
        }
    ]

    result = list(car_accidents.aggregate(pipeline))
    if result:
        return result[0]['total_accidents']
    else:
        return 0

def get_accidents_grouped_by_cause(beat_of_occurrence):
    pipeline = [
        {
            '$match': {
                'location.beat_of_occurrence': beat_of_occurrence
            }
        },
        {
            '$group': {
                '_id': '$prim_contributory_cause',
                'total_accidents': {'$sum': 1},
            }
        },
        {
            '$sort': {
                'total_accidents': -1
            }
        }
    ]
    results = list(car_accidents.aggregate(pipeline))
    return results




def get_accidents_statistics(beat_of_occurrence):



    pipeline = [
        {
            '$match': {
                'location.beat_of_occurrence': beat_of_occurrence

            }
        },
        {
            '$group': {
                '_id': None,
                'total_injuries': {'$sum': '$injuries.total'},
                'fatal_injuries': {'$sum': '$injuries.fatal'},
                'non_fatal_injuries': {'$sum': {'$subtract': ['$injuries.total', '$injuries.fatal']}},
                'events': {'$push': '$$ROOT'}
            }
        }
    ]


    result = list(car_accidents.aggregate(pipeline))


    if not result:
        return "No data found"


    return result[0]
