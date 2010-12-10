import xml.dom.minidom
import MySQLdb
import urllib
import sys
import time
import json
import pickle
import argparse
import datetime


parser = argparse.ArgumentParser(description='Gather NDFD data and put it into MySQL')
parser.add_argument('-r', dest='retry', action='store_true')
args = parser.parse_args()


# CONFIGURATION START
# List of NDFD Element eg. ['maxt', 'mint'] Leave [] for ALL variables
ndfd_elements = ['temp', 'dew', 'rh', 'pop12', 'qpf']

# Either time-series or glance
ndfd_product  = 'time-series'

# List of latitude and longitude tuples. MUST BE PROVIDED
coop_id       = [490, 350, 360, 330, 290, 251, 111]
locations     = [(27.22, -81.84), (27.76, -82.22), (28.02, -82.23), (28.10, -81.71), (29.22, -81.45), (28.75, -82.30), (28.02, -82.11)]

# Database configuration
db_host       = 'localhost'
db_port       = 3306
db_user       = 'user'
db_pass       = 'pass'
db_database   = 'database'

# DEBUGGING
debug = True
# CONFIGURATION END - PLEASE DO NOT EDIT PAST THIS POINT

oldpickledata = {}
runtime       = datetime.datetime.now()

try:
	cache_file = open('sqlcache.db', 'rb')
	oldpickledata = pickle.load(cache_file)
	cache_file.close()
except IOError:
	pass

if args.retry:
	if 'rerun' in oldpickledata:
		if not oldpickledata['rerun']:
			if debug:
				print "No need to re-run."
			sys.exit(0)
		

if 'data' in oldpickledata:
	oldpickledata = oldpickledata['data']

timemap   = {}
datamap   = {}
finaldata = {}


ndfd_url = "http://www.weather.gov/forecasts/xml/sample_products/browser_interface/ndfdXMLclient.php"

def gen_loc(head, tail):
	if(isinstance(head, tuple)):
		hlat, hlon = head
		tlat, tlon = tail
		return "%s,%s %s,%s" % (hlat, hlon, tlat, tlon)
	else:
		lat, lon = tail
		return "%s %s,%s" % (head, lat, lon)

if len(locations) == 0:
	err =  "ERROR: Invalid configuration: locations"
	sys.exit(err)
elif len(locations) == 1:
	ndfd_loc = "%.2f,%.2f" % locations[0]
else:
	ndfd_loc = urllib.quote(reduce(gen_loc, locations))

ndfd_el = '&'.join(map(lambda e: "%s=%s" % (e,e), ndfd_elements))
ndfd_url = "%s?listLatLon=%s&product=%s&%s" % (ndfd_url, ndfd_loc, ndfd_product, ndfd_el)


# Get the url and parse the information from it (using minidom)
response = urllib.urlopen(ndfd_url)
# Send the information to the parser
xml = xml.dom.minidom.parse(response)
response.close()

times = xml.getElementsByTagName('start-valid-time')
if len(times) == 0:
	oldpickedata['data'] = oldpickledata
	oldpickledata['rerun'] = True
	pickle_file = open('sqlcache.db', 'wb')
	pickle.dump(oldpickledata, pickle_file, -1)
	pickle_file.close()
	sys.exit("NDFD REST Service not responding at %s" % runtime.isoformat())

values = xml.getElementsByTagName('value')



def build_timemap(time):
	key = time.parentNode.firstChild.nextSibling.firstChild.nodeValue.strip()
	val = time.firstChild.nodeValue.strip()
	if key not in timemap:
		timemap[key] = list()
	timemap[key].append(val)

def build_datamap(data):
	loc = "%s" % str(locations[int(data.parentNode.parentNode.getAttribute('applicable-location')[-1:])-1])
	parent = data.parentNode.nodeName
	vartype   = '-'.join(data.parentNode.getAttribute('type').split())
	parent = '-'.join((vartype, parent))
	key    = data.parentNode.getAttribute('time-layout')
	val    = data.firstChild.nodeValue.strip()
	if loc not in datamap:
		datamap[loc] = {}
	if parent not in datamap[loc]:
		datamap[loc][parent] = {'key':key, 'vals':[]}
	datamap[loc][parent]['vals'].append(val)

def build_finaldata(loc):
	loc_index = locations.index(tuple(map(float, loc[1:-1].split(','))))
	location  = str(coop_id[loc_index])
	for varname, data in datamap[loc].iteritems():
		vardata = data['vals']
		timestamps = timemap[data['key']]
		for t,v in zip(timestamps, vardata):
			ts = str(int(time.mktime(time.strptime(t[:-6], "%Y-%m-%dT%H:%M:%S"))) - time.timezone)
			if location not in finaldata:
				finaldata[location] = {}
			if ts not in finaldata[location]:
				finaldata[location][ts] = {}
			finaldata[location][ts][varname] = v

# Building the values in-memory via maps
map(build_timemap, times)
map(build_datamap, values)
map(build_finaldata, datamap)

# Datbase interaction
sqlinsertdata = []
sqlupdatedata = []
newpickledata = {}


# Need the database object to cleanup the json object
db = MySQLdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_database)
for station_id, station_data in finaldata.iteritems():
	if station_id not in newpickledata:
		newpickledata[str(station_id)] = []
	for ts, time_data in finaldata[station_id].iteritems():
		newpickledata[str(station_id)].append(ts)
		time_data['timestamp'] = ts
		data = json.dumps(time_data)
		if len(oldpickledata) > 0 and (str(station_id) in oldpickledata):
			if ts in oldpickledata[str(station_id)]:
				sqlupdatedata.append((db.string_literal(data), str(station_id), str(ts)))
			else:
				sqlinsertdata.append((str(station_id), str(ts), db.string_literal(data)))
		else:
			sqlinsertdata.append((str(station_id), str(ts), db.string_literal(data)))

c  = db.cursor()
if len(sqlinsertdata) > 0:
	if debug:
		print "Inserting %i row(s)" % len(sqlinsertdata)
	c.executemany("""INSERT INTO strawberry_forecast(coop_id, forecast_ts, data) VALUES (%s, %s, %s)""", sqlinsertdata)
	db.commit()
if len(sqlupdatedata) > 0:
	if debug:
		print "Updating %i row(s)" % len(sqlupdatedata)
	c.executemany("""UPDATE strawberry_forecast SET data=%s WHERE coop_id=%s AND forecast_ts=%s""", sqlupdatedata)
	db.commit()
c.close()
db.close()

newpickledata['data'] = newpickledata
newpickledata['rerun'] = False

pickle_file = open('sqlcache.db', 'wb')
pickle.dump(newpickledata, pickle_file, -1)
pickle_file.close()