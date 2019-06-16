# An example Python client for Redis key-value store using RedisTimeSeries.
from redistimeseries.client import Client as RedisTimeSeries
import time
import sys
import site
import datetime
import random

print('\n'.join(sys.path))

ts = RedisTimeSeries(port=6379)

ts.flushdb()

begin_time = int(time.time())

ts.create('temperature', retentionSecs=60, labels={'sensor_id' : 2,'area_id' : 32})

begin_time_datetime = datetime.datetime.fromtimestamp(begin_time).strftime('%Y-%m-%d %H:%M:%S')

print("\nQuick insert of the data by start time: %s \n" % (begin_time_datetime))

for i in range(10):

	data = random.randint(i,100)

	sys.stdout.write('%d ' % (data))

	sys.stdout.flush()

	timestamp = int(time.time())

	ts.add('temperature',timestamp,str(data),retentionSecs=60,labels={'sensor_id' : 2,'area_id' : 32})

	time.sleep(1)

end_time = int(time.time())

end_time_datetime = datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')

print("\n\nQuery the data for a time range: %s to %s\n" % (begin_time_datetime, end_time_datetime))

for record in ts.range('temperature',begin_time,end_time,bucketSizeSeconds=1):

	timestamp = datetime.datetime.fromtimestamp(record[0]).strftime('%Y-%m-%d %H:%M:%S')

	if sys.version_info < (3, 0):
		data = unicode(record[1]).encode('utf8')
	else:
		data = record[1]

	print("%s : %s " % (timestamp, data))
