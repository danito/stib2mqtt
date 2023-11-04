import json
import pprint
import requests
import datetime
import yaml
import logging
import asyncio
import async_timeout
import aiohttp
from datetime import datetime
import pytz
import datetime
from datetime import timedelta
from datetime import timezone
from dateutil import parser
from paho.mqtt import client as mqtt_client
import random
import time

LOGGER = logging.getLogger(__name__)
with open('config.yml', 'r') as file:
    configuration = yaml.safe_load(file)

STIB_API = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets"
STIB_API_KEY = configuration['stib_api_key']
LANG = configuration['lang']
MESSAGE_LANG = configuration['message_lang']
STOP_NAMES = configuration['stop_names']
mqtt_server = configuration['mqtt_server']

mqtt_port = configuration['mqtt_port']
mqtt_user = configuration['mqtt_user']
mqtt_password = configuration['mqtt_password']
mqtt_topic = configuration['mqtt_topic']
client_id = f'stib-mqtt-{random.randint(0, 1000)}'
STOP_IDS = []
LINE_IDS = []
TOPIC = "homeassistant/sensor/" 
MQTT_DATA = {}

def convert_to_utc(localtime, timeformat):
    """Convert local time of Europe/Brussels of the API into UTC."""
    if localtime is None:
        return None
    if timeformat is None:
        timeformat = "%Y-%m-%dT%H:%M:%S"
    localtimezone = pytz.timezone("Europe/Brussels")
    localtimenaive = datetime.strptime(localtime, timeformat)
    dtlocal = localtimezone.localize(localtimenaive)
    dtutc = dtlocal.astimezone(pytz.utc)
    return dtutc
def setAttributes(stop_id, line_id, pt_id, attributes):
    if stop_id in ATTRIBUTES:
        if line_id in ATTRIBUTES[stop_id]:
            ATTRIBUTES[stop_id][line_id][pt_id].update(attributes)
        else:
            ATTRIBUTES[stop_id][line_id][pt_id] = attributes
    else:
        ATTRIBUTES[stop_id] = {[line_id][pt_id] : attributes}
def setConfig(stop_id, line_id, config):
    if stop_id in CONFIG:
        if line_id in CONFIG[stop_id]:
            CONFIG[stop_id][line_id].update(config)
        else:
            CONFIG[stop_id][line_id] = config
    else:
        CONFIG[stop_id] = {[line_id] : config}
def setState(stop_id, line_id, state):
    if stop_id in STATES:
        if line_id in STATES[stop_id]:
            STATES[stop_id][line_id].update(state)
        else:
            STATES[stop_id][line_id] = state
    else:
        STATES[stop_id] = {[line_id] : state}

def diff_in_minutes(t):
    now = pytz.utc.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    iso = datetime.datetime.fromisoformat(t)
    tmp = pytz.utc.normalize(iso)
    return round( (tmp-now).total_seconds()/60)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.username_pw_set(mqtt_user, mqtt_password)
    client.on_connect = on_connect
    client.connect(mqtt_server, int(mqtt_port))
    return client
class StibData:
    """A class to get passage information."""

    def __init__(self, session=None):
        """Initialize the class."""
        self.session = session
        self.stib_api = STIBApi()
        self.stop_ids= []
        self.stop_fields = {}
        self.state = {}
        self.config = {}
        self.attributes = {}

    async def get_stopIds(self, stopnames):
        """Get the stop ids from the stop name."""
        stop_names = []
        for stop, stop_name in enumerate(stopnames):
               stop_names.append(stop_name)
        q = " OR ".join(' name like "' + item + '"' for item in stop_names)
        dataset='stop-details-production'
        stop_data = await self.stib_api.get_stib_data(dataset, q)
        if stop_data is not None and 'results' in stop_data:
            for r in stop_data['results']:
                stop_id = r['id']
                k = ''.join(i for i in str(stop_id) if i.isdigit())
                k = str(k)
                stop_name = json.loads(r["name"])
                stop_gps  = json.loads(r['gpscoordinates'])
                self.stop_ids.append(k)
                self.stop_fields[k] = {"stop_id" : stop_id, "stop_names" : stop_name, "gps_coordinates" : stop_gps }
        return {"stop_ids" : self.stop_ids, "stop_fields" : self.stop_fields}
    
    async def get_passing_times(self, stop_ids):
        """Get the passing time data from the stop ids."""
        q = " OR ".join('pointid like "' + ''.join(i for i in str(item) if i.isdigit()) + '"' for item in stop_ids)
        dataset='waiting-time-rt-production'
        stib_data = await self.stib_api.get_stib_data(dataset, q)
        line_ids = []
        passing_times = {}
        stops = {}
        if stib_data is not None and 'results' in stib_data:
            for r in stib_data['results']:
                passingtimes = json.loads(r['passingtimes'])
                stop_id = str(r['pointid'])
                line_id = str(r['lineid'])
                line_ids.append(line_id)
                stop_pt = []
                x = 0
                
                for pt in passingtimes:
                    x = x + 1
                    key ="NstibL"+ line_id + stop_id + str(x)
                    t = pt["expectedArrivalTime"]
                    pt['minutes'] = diff_in_minutes(t)
                    stop_pt.append(pt)
                    message = ""
                    end_of_service = False
                    if "message" in pt:
                        mesage = pt['message'][MESSAGE_LANG]
                        if len(passing_times) < 2:
                            end_of_service = True
                    destination = ""
                    if "destination" in pt:
                        destination = pt["destination"][LANG]
                    pt['attributes'] =  {                   
                    "message" : message, 
                    "destination" : destination,
                    "line_id" : line_id,
                    "stop_id" : stop_id,
                    "end_of_service" : end_of_service
                    }                        
                    
                    
                if stop_id not in passing_times:
                    passing_times[stop_id] = {line_id : stop_pt}
                else:
                    passing_times[stop_id].update({line_id : stop_pt})
        return {"line_ids" : line_ids, "passing_times" : passing_times}
    
    async def get_lines_by_stops(self, stop_ids):
        """Get lines by stop ids."""
        q = " OR ".join('points like "' + ''.join(i for i in str(item) if i.isdigit()) + '"' for item in stop_ids)
        dataset='stops-by-line-production'
        stib_data = await self.stib_api.get_stib_data(dataset, q)
        lines = []
        line_details = {}
        if stib_data is not None and 'results' in stib_data:
            for r in stib_data['results']:
                line_id = str(r['lineid'])
                direction = r['direction']
                destination = json.loads(r['destination'])
                lines.append(line_id)
                line_details[line_id] = {'line_id': line_id, 'direction': direction, 'destination': destination}
        return {'lines': lines, 'line_details' : line_details}
    
    async def get_routes_by_lines(self, line_ids):
        """Get route details by line ids."""
        q = " OR ".join('route_short_name like "' + ''.join(i for i in str(item) if i.isdigit()) + '"' for item in line_ids)
        dataset='gtfs-routes-production'
        stib_data = await self.stib_api.get_stib_data(dataset, q)
        lines = []
        route_details = {}
        if stib_data is not None and 'results' in stib_data:
            for r in stib_data['results']:
                line_id = str(r['route_short_name'])
                route_long_name = r['route_long_name']
                route_type = r['route_type']
                route_color = r['route_color']
                route_details[line_id] = {'route_short_name': line_id, 'route_long_name' : route_long_name, 'route_type' : route_type, 'route_color' : route_color }
        return route_details
    
class STIBApi:
    async def get_stib_data(self, dataset, query, session=None):
        selfcreatedsession = False
        self.session = session
        result = None
        if self.session is None:
            selfcreatedsession = True
        params = dict(
            where=query,
            start=0,
            rows=99,
            apikey = STIB_API_KEY
         )
        endpoint = "{}/{}/records".format(
            STIB_API, dataset
        )
        common = CommonFunctions(self.session)
        result = await common.api_call(endpoint, params)
        if selfcreatedsession is True:
                await common.close()
        return result
        
class CommonFunctions:
    """A class for common functions. """
    """inspired by pydelijn. """
    def __init__(self, session):
        """Initialize the class."""
        self.session = session

    async def api_call(self, endpoint, params):
        """Call the API."""
        data = None
        if self.session is None:
            self.session = aiohttp.ClientSession()
        try:
            async with async_timeout.timeout(5):
                LOGGER.debug("Endpoint URL: %s", str(endpoint))
                response = await self.session.get(url=endpoint, params=params)
                if response.status == 200:
                    try:
                        data = await response.json()
                    except ValueError as exception:
                        message = "Server gave incorrect data"
                        raise Exception(message) from exception

                elif response.status == 401:
                    message = "401: Acces token might be incorrect"
                    raise HttpException(message, await response.text(), response.status)

                elif response.status == 404:
                    message = "404: incorrect API request"
                    raise HttpException(message, await response.text(), response.status)

                else:
                    message = f"Unexpected status code {response.status}."
                    raise HttpException(message, await response.text(), response.status)

        except aiohttp.ClientError as error:
            LOGGER.error("Error connecting to Stib API: %s", error)
        except asyncio.TimeoutError as error:
            LOGGER.debug("Timeout connecting to Stib API: %s", error)
        return data

    async def close(self):
        """Close the session."""
        await self.session.close()


class HttpException(Exception):
    """HTTP exception class with message text, and status code."""

    def __init__(self, message, text, status_code):
        """Initialize the class."""
        super().__init__(message)
        self.status_code = status_code
        self.text = text

def init():

    stop_ids = asyncio.run(StibData().get_stopIds(STOP_NAMES))
    stop_fields = stop_ids["stop_fields"]
    for s in stop_ids['stop_ids']:
        if s not in STOP_IDS:
            STOP_IDS.append(str(s))
   
    pt = asyncio.run(StibData().get_passing_times(stop_ids['stop_ids']))
    for l in pt['line_ids']:
        if l not in LINE_IDS:
            LINE_IDS.append(str(l))
        
    lines = asyncio.run(StibData().get_lines_by_stops(stop_ids['stop_ids']))
    routes = asyncio.run(StibData().get_routes_by_lines(pt['line_ids']))
    passing_times = pt['passing_times']
    #print(json.dumps(passing_times))
    attributes = {                   
                        "message" : None, 
                        "destination" : None,
                        "device_class": None,
                        "timestamp" : None,
                        "line_id" : None,
                        "stop_id" : None,
                        "end_of_service" : None,
                        "route_color" : None,
                        "route" : None,
                        'route_type': None,
                        "longitude" : None,
                        "latitude": None,
                        }                        
                    
    config = {
                        "icon" : "mdi:bus",                        
                        "device_class": "duration",
                        "json_attributes_template": "{{value_json | default('') | to_json}}",
                        "json_attributes_topic": None,
                        "state_topic": None,
                        "command_topic": None,
                        "unique_id": None,
                        "unit_of_measurement": 'min',
                        "value_template": '{{value_json.arrival}}',
                        "device" : {}
                    }
    state =  {
                        "arrival": None
                    }
    
    for s in STOP_IDS:
        stop_name = stop_fields[s]['stop_names'][LANG]
        latitude = stop_fields[s]["gps_coordinates"]["latitude"]
        longitude = stop_fields[s]["gps_coordinates"]["longitude"]
        if s in passing_times:
            for i,l in passing_times[s].items():
                x = 0 
                for idx, p in enumerate(l):
                    x = 0                    
                    x = x + 1
                    key = "stibL" + str(i) + str(s) + str(x)
                    c = config.copy()
                    c.update(
                        {
                            "json_attributes_topic": TOPIC + key + "/attribute",
                            "state_topic": TOPIC + key + "/state",
                            "command_topic": TOPIC + key + "/set",
                            "unique_id": key,
                        }
                    )
                    route = ""
                    route_color = ""
                    route_type = "bus"
                    if i in routes:
                        route = routes[i]["route_long_name"]
                        route_color = routes[i]["route_color"]
                        route_type = routes[i]["route_type"]

                    c.update({
                        "icon" : "mdi:" + route_type.lower(),
                        "device" : {
                            "identifiers" : [key],
                            "name" : "STIB " + stop_name + " (" + str(s) + ") " + route_type + " " + i + " " + str(x)
                        }
                        
                        })
                    p['attributes'].update(
                        {
                            "stop_name" : stop_name,
                            "latitude" : latitude,
                            "longitude" : longitude,
                            "route" : route,
                            'route_color' : route_color,
                            'timestamp' :  p["expectedArrivalTime"],
                            'passage' : x
                        }
                    )
                    m = {
                        "arrival" : diff_in_minutes(p["expectedArrivalTime"])
                    }
                    if key not in MQTT_DATA:
                        MQTT_DATA[key] ={
                            "attributes" : p["attributes"],
                            "config" : c,
                            "state" : m
                            }
                    else:
                        if "attributes" not in MQTT_DATA[key]:
                            MQTT_DATA[key]['attributes'] = p["attributes"].copy()
                            MQTT_DATA[key]['config'] = c.copy()
                            MQTT_DATA[key]['state'] = m.copy()
                        else:
                            MQTT_DATA[key]['attributes'].update(p["attributes"])
                            MQTT_DATA[key]['config'].update(c)
                            MQTT_DATA[key]['state'].update(m)

def publish(client):
    cc = 0
    check_stops = False
    for idx, mq in MQTT_DATA.items():
        config_topic = TOPIC + str(idx) + "/config"
        state_topic = TOPIC + str(idx) + "/state"
        attribute_topic = TOPIC + str(idx) + "/attribute"
        j_config = json.dumps(mq["config"], indent=4, sort_keys=True, ensure_ascii=False)
        #j_config  = "" #to delete entities from HA

        j_attribute = json.dumps(mq["attributes"], indent=4,sort_keys=True, ensure_ascii=False)
        j_state = json.dumps(mq["state"], indent=4, sort_keys=True, ensure_ascii=False)
        r_config = client.publish(config_topic, j_config, qos=0, retain=True)
        client.publish(attribute_topic, j_attribute, qos=0, retain=True)
        client.publish(state_topic, j_state, qos=0, retain=False)
        status = r_config[0]
        if status == 0:
            print(f"Send msg to topic `{config_topic}`")
        else:
            print(f"Failed to send message to topic {config_topic}")

    while True:
        pt = asyncio.run(StibData().get_passing_times(STOP_IDS))
        passing_times = pt['passing_times']
        for idx, mq in MQTT_DATA.items():
            stop_id = mq["attributes"]["stop_id"]
            line_id = mq["attributes"]["line_id"]
            print(stop_id, line_id)
            pt = passing_times[stop_id][line_id]
            x=0
            for p in pt:
                x = x + 1
                if x != mq["attributes"["passage"]:
                    continue
                m = {
                    "arrival" : diff_in_minutes(p["expectedArrivalTime"])
                }
                mq['state'] = m
                state_topic = TOPIC + idx + "/state"
                config_topic = TOPIC + idx + "/config"
                attribute_topic = TOPIC + idx + "/attributes"
                j_state = json.dumps(m, indent=4, sort_keys=True, ensure_ascii=False)
                j_attributes = json.dumps(mq['attributes'], indent=4, sort_keys=True, ensure_ascii=False)
                #r_config = client.publish(config_topic, "", qos=0, retain=True)
                response = client.publish(state_topic, j_state, qos=0, retain=False)
                status = response[0]
                if status != 0:
                    print(f"Failed to send message to topic {state_topic}")
                response = client.publish(attribute_topic, j_attributes, qos=0, retain=False)
                status = response[0]
                if status != 0:
                    print(f"Failed to send message to topic {attribute_topic}")

        """ Reload data at 2:00, 8:00 & 18:00 """
        now = datetime.datetime.now()
        current_time = now.strftime("%H:%M:%S")
        if current_time < "02:00:00":
            check_stops = True
        if current_time >= "02:00:00" and check_stops:
            print("Reloading data 02:00")
            init()
            check_stops = False
        if current_time < "08:00:00":
            check_stops = True
        if current_time >= "08:00:00" and check_stops:
            print("Reloading data 08:00")
            init()
            check_stops = False
        if current_time < "18:00:00":
            check_stops = True
        if current_time >= "18:00:00" and check_stops:
            print("Reloading data 18:00")
            init()
            check_stops = False
        time.sleep(30)
    
def mq_config():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

if __name__ == "__main__":
    init()
    mq_config()

"""
line[id] = []
"""
