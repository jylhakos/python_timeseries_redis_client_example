# An example Python client for Redis key-value store using RedisTimeSeries.
from redistimeseries.client import Client as RedisTimeSeries
import time
import sys
import site
import datetime
import random

print(' \n '.join(sys.path))

ts = RedisTimeSeries(host='localhost', port=6379)

ts.flushdb()

key = 'temperature'

def query(key, begin_time, end_time):
	try:
		for record in ts.range(key,begin_time,end_time,bucketSizeSeconds=1):

			timestamp = datetime.datetime.fromtimestamp(record[0]).strftime('%Y-%m-%d %H:%M:%S')
			
			data = round(float(record[1]),2)

			print(' %s : %.2f ' % (timestamp, data))
	except Exception:
		print("\n Error")

begin_time = int(time.time())

ts.create(key,retentionSecs=30,labels={'sensor_id' : 2,'area_id' : 32})

begin_time_datetime = datetime.datetime.fromtimestamp(begin_time).strftime('%Y-%m-%d %H:%M:%S')

print("\n Quick insert of the data by start time: %s \n" % (begin_time_datetime))

for i in range(10):

	data = round(random.uniform(0.0,100.0),2)

	timestamp = int(time.time())

	timestamp_strftime = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

	sys.stdout.write(' %s : %.2f \n' % (timestamp_strftime, data))

	sys.stdout.flush()

	ts.add(key,timestamp,data,retentionSecs=30, labels={'sensor_id' : 2,'area_id' : 32})

	time.sleep(1)

end_time = int(time.time())

end_time_datetime = datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')

time.sleep(1)

print("\n Query the data for a time range: %s to %s\n" % (begin_time_datetime, end_time_datetime))

query(key,begin_time,end_time)

ts.delete(key)

print('')
