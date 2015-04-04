import requests
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import sqlite3 as lite
import time
from dateutil.parser import parse 
import collections


# ------------------------
# GET DATA AND PARSE JSON  
# ------------------------

# get Citibike data:
r = requests.get('http://www.citibikenyc.com/stations/json')

# put JSON documents into a dataframe
df = json_normalize(r.json()['stationBeanList'])


# -----------------
# STORE STATIC DATA
# -----------------

# connect to database
con = lite.connect('citi_bike.db')
cur = con.cursor()

# clear tables if they exist
with con:
	# clear tables if they exist
	cur.execute("DROP TABLE IF EXISTS citibike_reference")
	cur.execute("DROP TABLE IF EXISTS available_bikes")

# fill static data
with con:

	# create table for the citibike data
	sql = ("CREATE TABLE citibike_reference (id INT PRIMARY KEY, " 
    	"totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, "
    	"longitude NUMERIC, postalCode TEXT, testStation TEXT, "
    	"stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, "
    	"location TEXT )")
    cur.execute(sql)

    sql = ("INSERT INTO citibike_reference (id, totalDocks, "
    	"city, altitude, stAddress2, longitude, postalCode, "
    	"testStation, stAddress1, stationName, landMark, "
    	"latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")

    # fill table with data in dataframe
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        fill = (
        	station['id'],station['totalDocks'],
        	station['city'],station['altitude'],station['stAddress2'],
        	station['longitude'],station['postalCode'],
        	station['testStation'],station['stAddress1'],
        	station['stationName'],station['landMark'],
        	station['latitude'],station['location']
        	)
        cur.execute(sql, fill)

# extract the station ids from the DataFrame 
station_ids = df['id'].tolist() 

# add the '_' to the station name and add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

# create the available bikes table
# in this case, we're concatentating the string and joining all 
# the station ids (now with '_' and 'INT' added)
with con:
	sql = ("CREATE TABLE available_bikes "
		   "(execution_time INT, " +  ", ".join(station_ids) + ");")
    cur.execute(sql)


# ------------------
# STORE DYNAMIC DATA
# ------------------

# collect available bike data for one hour
for i in range(60):

    #take the execution time string and parse into a datetime object
	exec_time = parse(r.json()['executionTime'])

	# create entry for the execution time
	with con:
		sql = 'INSERT INTO available_bikes (execution_time) VALUES (?)'
		cur.execute(sql, (exec_time.strftime('%s'),))

	#defaultdict to store available bikes by station
	id_bikes = collections.defaultdict(int) 

	#loop through the stations in the station list
	for station in r.json()['stationBeanList']:
		id_bikes[station['id']] = station['availableBikes']

	#iterate through the defaultdict to update the values in the database
	with con:
		for k, v in id_bikes.iteritems():
			sql = ("UPDATE available_bikes SET _" + str(k) + 
				   " = " + str(v) + " WHERE execution_time = " + 
				   exec_time.strftime('%s') + ";")
			cur.execute(sql)

	# wait sixty seconds
	time.sleep(60)

	# get new data
	r = requests.get('http://www.citibikenyc.com/stations/json')
