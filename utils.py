import calendar
import json
import pytz
import time
import uuid

from base64 import b64encode
from collections import OrderedDict, namedtuple
from datetime import datetime

from cassandra.util import Date, OrderedMapSerializedKey, SortedSet

from cassandra_udts import Averages, Description, Livewebcam, Position, Thumbnails


def datetime_to_timestamp_ms(dt):
	return calendar.timegm(dt.utctimetuple()) * 1e3

def make_timestamp_range(from_ts, to_ts):
    Timerange = namedtuple('Timerange', 'from_timestamp to_timestamp')

    from_timestamp = from_ts
    to_timestamp = to_ts
	
    if not from_timestamp and not isinstance(from_timestamp, int):
        from_timestamp = 0
    if not to_timestamp and not isinstance(to_timestamp, int):
        to_date_dt = datetime.utcnow()
        to_timestamp = datetime_to_timestamp_ms(to_date_dt)
    
    return Timerange(from_timestamp=from_timestamp, to_timestamp=to_timestamp)

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, OrderedMapSerializedKey):
            return {str(k):v for k,v in obj.items()}
        elif isinstance(obj, OrderedDict):
            return {str(k):v for k,v in obj.items()}
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, Averages):
            return {'min_value': obj.min_value, 'avg_value': obj.avg_value,
                'max_value': obj.max_value, 'unit': obj.unit}
        elif isinstance(obj, Description):
            return {'short_description': obj.short_description, 'long_description': obj.long_description}
        elif isinstance(obj, Livewebcam):
            return {'url': obj.url, 'ip_address': obj.ip_address}
        elif isinstance(obj, Position):
            return {'latitude': obj.latitude, 'longitude': obj.longitude}
        elif isinstance(obj, datetime):
            timestamp_utc = pytz.utc.localize(obj)
            return int(timestamp_utc.timestamp()) * 1e3
        elif isinstance(obj, Date):
            return obj.seconds
        elif isinstance(obj, bytes):
            base64_bytes = b64encode(obj)
            base64_string = base64_bytes.decode('utf-8')
            return base64_string
        elif isinstance(obj, Thumbnails):
            return {
                'xl': b64encode(obj.xl).decode('utf-8'),
                'l': b64encode(obj.l).decode('utf-8'),
                'm': b64encode(obj.m).decode('utf-8'),
                's': b64encode(obj.s).decode('utf-8')
            }
        elif isinstance(obj, SortedSet):
            return [i for i in SortedSet]
