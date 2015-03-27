import pandas as pd
import sqlite3 as lite
import collections
import matplotlib.pyplot as plt
import datetime


# connect to the database
con = lite.connect('citi_bike.db')
cur = con.cursor()

# put retrieved data in a dataframe
df = pd.read_sql_query("SELECT * FROM available_bikes \
	ORDER BY execution_time",con,index_col='execution_time')


# -----------------------------
# STATION WITH THE MAX ACTIVITY
# -----------------------------

# calculate the acitvity (i.e., total change) for each station
total_change = collections.defaultdict(int)

# loop over stations
for column in df.columns:

	# list of minutely available bikes for the particular station
	bikenumlist = df[column].tolist()

	# aggregate change
	bikenumchange = 0;
	for i in range(len(bikenumlist)-1):
		bikenumchange += abs(bikenumlist[i] - bikenumlist[i+1])
		total_change[column] = bikenumchange

# find the station with the max activity
max_station = max(total_change, key=total_change.get)

# print info
#query sqlite for reference information
with con:
	cur.execute("SELECT id, stationname, latitude, longitude \
		FROM citibike_reference WHERE id = " + max_station.lstrip('_'))
	data = cur.fetchone()

print "The most active station is station id %s at %s latitude: %s longitude: %s " % data
print "With " + str(total_change[max_station]) + " bicycles coming and going in the hour between " + datetime.datetime.fromtimestamp(int(df.index[0])).strftime('%Y-%m-%dT%H:%M:%S') + " and " + datetime.datetime.fromtimestamp(int(df.index[-1])).strftime('%Y-%m-%dT%H:%M:%S')


# -----------------------------
# PLOTTING
# -----------------------------
plt.bar(range(len(df.columns)), total_change.values())
plt.xlabel('stations')
plt.ylabel('activity')
plt.draw()
plt.savefig('bike_change.png')
plt.show()