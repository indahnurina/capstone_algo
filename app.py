from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 


@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/average_duration')
def route_all_trips_avg():
    conn = make_connection()
    avg = get_all_trips_avg(conn)
    return str(avg)

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/trips/<trip_id>')
def route_trip_id(trip_id):
    conn = make_connection()
    station = get_trip_id(trip_id, conn)
    return station.to_json()

@app.route('/trips/average_duration/<bike_id>')
def route_bikeid_trips_avg(bike_id):
    conn = make_connection()
    avg = get_bikeid_trips_avg(bike_id,conn)
    return str(avg)

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/getinfo',methods=["POST"]) 
def getinfo():
    input_data = request.get_json(force=True) # Parse the incoming json data as Dictionary
    specified_date = input_data['period'] 
    return agg(specified_date ).to_json()

 ########################################## FUNCTION #######################################   
def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_trips_avg(conn):
    query = f"""SELECT duration_minutes FROM trips"""
    result = pd.read_sql_query(query, conn)
    result.duration_minutes[result.duration_minutes==0]=result.duration_minutes.median()
    result = result.duration_minutes.mean()
    return result 

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_bikeid_trips_avg(bike_id,conn):
    query = f"""SELECT duration_minutes FROM trips WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    result.duration_minutes[result.duration_minutes==0]=result.duration_minutes.median()
    result = result.duration_minutes.mean()
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try: 
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def agg(specified_date):
    conn = make_connection()
    query = f"""SELECT * FROM trips WHERE start_time LIKE ("{specified_date}%")"""
    selected_data = pd.read_sql_query(query, conn)

    result = selected_data.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'})
    return result

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

if __name__ == '__main__':
    app.run(debug=True, port=5000)