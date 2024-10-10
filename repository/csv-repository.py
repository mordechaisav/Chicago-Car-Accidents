from database.connect import car_accidents,locations
from utils.csv_util import read_csv,parse_date
from datetime import datetime

CSV_PATH = "../data/Traffic_Crashes.csv"


def get_or_create_location(location_data, location_collection):

    existing_location = location_collection.find_one({
        'street_no': location_data['street_no'],
        'street_direction': location_data['street_direction'],
        'street_name': location_data['street_name'],
        'beat_of_occurrence': location_data['beat_of_occurrence'],
        'lat': location_data['lat'],
        'long': location_data['long'],
        'location': location_data['location'],
    })
    if existing_location:
        return existing_location['_id']

    location_id = location_collection.insert_one(location_data).inserted_id
    return location_id



def init_crash_data():
   car_accidents.drop()
   locations.drop()


   for row in read_csv(CSV_PATH):
       location = {
           'street_no': row['STREET_NO'],
           'street_direction': row['STREET_DIRECTION'],
           'street_name': row['STREET_NAME'],
           'beat_of_occurrence': row['BEAT_OF_OCCURRENCE'],
           'lat': row['LATITUDE'],
           'long': row['LONGITUDE'],
           'location': row['LOCATION']
       }
       location_id = get_or_create_location(location,locations)
       injuries = {
           'most_severe_injury': row['MOST_SEVERE_INJURY'],
           'total': row['INJURIES_TOTAL'],
           'fatal': row['INJURIES_FATAL'],
           'incapacitating': row['INJURIES_INCAPACITATING'],
           'non_incapacitating': row['INJURIES_NON_INCAPACITATING'],
           'reported_not_evident': row['INJURIES_REPORTED_NOT_EVIDENT'],
           'no_indication': row['INJURIES_NO_INDICATION'],
           'unknown': row['INJURIES_UNKNOWN'],
       }
       crash_date = parse_date(row['CRASH_DATE'])
       crash_year = crash_date.year
       crash_month = crash_date.month
       date = {
           'date': crash_date,
           'year': crash_year,
           'month': crash_month,
           'day_of_week': int(row['CRASH_DAY_OF_WEEK'])
       }
       crash = {
           'crash_record_id': row['CRASH_RECORD_ID'],
           'crash_date': date,
           'crash_type': row['CRASH_TYPE'],
           'intersection_related_i': row['INTERSECTION_RELATED_I'],
           'not_right_of_way_i': row['NOT_RIGHT_OF_WAY_I'],
           'hit_and_run_i': row['HIT_AND_RUN_I'],
           'damage': row['DAMAGE'],
           'date_police_notified': row['DATE_POLICE_NOTIFIED'],
           'prim_contributory_cause': row['PRIM_CONTRIBUTORY_CAUSE'],
           'sec_contributory_cause': row['SEC_CONTRIBUTORY_CAUSE'],
           'location_id': location_id,
           'crash_hour': row['CRASH_HOUR'],


           'num_units': row['NUM_UNITS'],
           'injuries': injuries
       }

       car_accidents.insert_one(crash)
init_crash_data()