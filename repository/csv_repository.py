from database.connect import car_accidents
from utils.csv_util import read_csv,parse_date,safe_int
from datetime import datetime

CSV_PATH = "data/Traffic_Crashes.csv"




def init_crash_data(collection,csv_path):
   car_accidents.drop()


   for row in read_csv(csv_path):
       location = {
           'street_no': row['STREET_NO'],
           'street_direction': row['STREET_DIRECTION'],
           'street_name': row['STREET_NAME'],
           'beat_of_occurrence': row['BEAT_OF_OCCURRENCE'],
           'lat': row['LATITUDE'],
           'long': row['LONGITUDE'],
           'location': row['LOCATION']
       }
       injuries = {
           'most_severe_injury': row['MOST_SEVERE_INJURY'],
           'total': safe_int(row.get('INJURIES_TOTAL')),
           'fatal': safe_int(row.get('INJURIES_FATAL')),
           'incapacitating': safe_int(row.get('INJURIES_INCAPACITATING')),
           'non_incapacitating': safe_int(row.get('INJURIES_NON_INCAPACITATING')),
           'reported_not_evident': safe_int(row.get('INJURIES_REPORTED_NOT_EVIDENT')),
           'no_indication': safe_int(row.get('INJURIES_NO_INDICATION')),
           'unknown': safe_int(row.get('INJURIES_UNKNOWN')),
       }
       crash_date = parse_date(row['CRASH_DATE'])
       crash_year = crash_date.year
       crash_month = crash_date.month
       date = {
           'date': crash_date.replace(hour=0, minute=0, second=0, microsecond=0),
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
           'crash_hour': row['CRASH_HOUR'],
           'location': location,
           'num_units': row['NUM_UNITS'],
           'injuries': injuries
       }

       collection.insert_one(crash)
