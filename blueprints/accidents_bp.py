from bson import ObjectId
from flask import Flask,Blueprint,jsonify,request
from repository.crash_reposintory import *
from repository.index_repo import initialize_index
from database.connect import car_accidents
from repository.csv_repository import init_crash_data
CSV_PATH = "data/Traffic_Crashes.csv"
accidents_bp = Blueprint('accidents', __name__)


@accidents_bp.route('/init_database', methods=['GET'])
def init_database():
    init_crash_data(car_accidents,CSV_PATH)
    return jsonify({"message": "Database initialized"}), 200


@accidents_bp.route('/count_accidents_by_area', methods=['GET'])
def count_accidents_by_area_route():
    area = request.args.get('area')
    count = count_accidents_by_area(area)
    return jsonify({"count": count}), 200



@accidents_bp.route('/count_accidents_by_time_and_area', methods=['GET'])
def count_accidents_by_time_and_area_route():
    area = request.args.get('area')
    time_period = request.args.get('time_period')
    time_range = request.args.get('time_range')
    count = count_accidents_by_time_and_area(area,time_period ,time_range)
    return jsonify({"count": count}), 200


@accidents_bp.route('/accidents_grouped_by_cause', methods=['GET'])
def accidents_grouped_by_cause_route():
    area = request.args.get('area')
    accidents_grouped = get_accidents_grouped_by_cause(area)
    return jsonify(accidents_grouped), 200


@accidents_bp.route('/accidents_statistics', methods=['GET'])
def accidents_statistics_route():
    area = request.args.get('area')
    statistics = get_accidents_statistics(area)
    for statistic in statistics["events"]:
        statistic["_id"] = str(statistic["_id"])
    return jsonify(statistics), 200


@accidents_bp.route('/create_index', methods=['GET'])
def create_index_route():
    time_gap = initialize_index()
    return jsonify({"message": "Index created",'time gap':time_gap.seconds}), 200

