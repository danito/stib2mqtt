import json
import pprint
import requests
import datetime
from datetime import timedelta
from datetime import timezone
from dateutil import parser
import pytz
from paho.mqtt import client as mqtt_client
import random
import time
import yaml

with open('config.yml', 'r') as file:
    configuration = yaml.safe_load(file)

STIB_API = "https://data.stib-mivb.be/api/records/1.0/search/"

STIB_API_KEY = configuration['stib_api_key']
mqtt_server = configuration['mqtt_server']

mqtt_port = configuration['mqtt_port']
mqtt_user = configuration['mqtt_user']
mqtt_password = configuration['mqtt_password']
mqtt_topic = configuration['mqtt_topic']
client_id = f'stib-mqtt-{random.randint(0, 1000)}'

STOPS = configuration['stops']

LANG = configuration['lang']
MESSAGE_LANG = configuration['message_lang']

def check_live(url):
    #print(url)
    try:
        #print('try')
        r = requests.get(url)
        live = r.ok
        #print(r.status_code)
        if (r.status_code == 400):
           live = True
    except requests.ConnectionError as e:
        print("ERROR")

        print(e)
        if 'MaxRetryError' not in str(e.args) or 'NewConnectionError' not in str(e.args):
            raise
        if "[Errno 8]" in str(e) or "[Errno 11001]" in str(e) or ["Errno -2"] in str(e):
            print('DNSLookupError')
            live = False
        else:
            raise
    except:
        raise
    #print(live)
    return live

def getStibData(q, dataset):
    url = STIB_API
    params = dict(
    dataset = dataset,
    q=q,
    start=0,
    rows=99,
    apikey = STIB_API_KEY
    )
    data = []
    try:
        r = requests.get(url=url, params=params)
        data = r.json()
        live = r.ok
    except requests.ConnectionError as e:

        print("ERROR in StibData")
        if 'MaxRetryError' not in str(e.args) or 'NewConnectionError' not in str(e.args):
            print("MaxRetry")
            raise
        if "[Errno 8]" in str(e) or "[Errno 11001]" in str(e) or "[Errno -2]" or "[Errno 3]" in str(e):
            print('DNSLookupError')
            live = False
        else:
            raise
    except:
        raise
    #print(live)
    return data

def getStopInfos():
    stopIds = []
    lineIds = []
    StopFields = {}
    LineFields = {}
    RouteFields = {}
    for stopId, stop in enumerate(STOPS):
        stopIds.append(stop['stop_id'])
        k = "STOP"+str(stop['stop_id'])
        StopFields[k] = {"stop_id":stop['stop_id']}
        for line_id in stop['line_numbers']:
            if line_id not in lineIds:
                keyName= "L" + str(line_id)
                LineFields[keyName]={"line_id":line_id}
                RouteFields[keyName]={"line_id":line_id}
                lineIds.append(line_id)

    q = " OR ".join(str(item) for item in stopIds)
    dataset='stop-details-production',
    StopData = getStibData(q, dataset)
    if StopData and "records" in StopData:
        StopRecords = StopData['records']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            stopsName  = json.loads(r["fields"]["name"])
            stopId = json.loads(r["fields"]["id"])
            gpscoordinates = json.loads(r["fields"]['gpscoordinates'])
            k = "STOP" + str(stopId)
            StopFields[k]["stop_names"] = stopsName
            StopFields[k]["gps_coordinates"] = gpscoordinates
    else: return []

    q = " OR ".join(str(item) for item in lineIds)
    dataset='stops-by-line-production'
    StopData = getStibData(q, dataset)
    if "records" in StopData:
        StopRecords = StopData['records']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            lineId  = r["fields"]["lineid"]
            k = "L" +  str(lineId)
            if k in LineFields:
                destination = (r["fields"]["destination"])
                direction = (r["fields"]["direction"])
                LineFields[k]["destination"] = destination
                LineFields[k]["direction"] = direction

    dataset='gtfs-routes-production'
    RoutesData = getStibData(q, dataset)
    if "records" in RoutesData:
        RoutesRecords = RoutesData['records']
    else:
        RoutesRecords = False
    if RoutesRecords:
        for r in RoutesRecords:
            lineId  = r["fields"]["route_short_name"]
            k = "L" +  str(lineId)
            if k in LineFields:
                RouteFields[k]["route_type"] = r['fields']['route_type']
                RouteFields[k]["route_color"] = r['fields']['route_color']

    return {"stops": StopFields, "lines": LineFields, "routes": RouteFields}


def getWaitingTimes(fields):
    #print(fields)
    if (len(fields) == 0 ):
        return False
    now = pytz.utc.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    stopIds = []
    lineIds = []
    StopFields = fields['stops']
    #print(StopFields)
    LineFields = fields['lines']
    RouteFields = fields['routes']
    WaitingTimeFields = {}
    for stopId, stop in enumerate(STOPS):
        stopIds.append(stop['stop_id'])
        k = str(stop['stop_id'])
        for line_id in stop['line_numbers']:
            keyName= "L" + str(line_id)
            stopName = StopFields['STOP' + k]['stop_names'][LANG]
            routeType = RouteFields[keyName]["route_type"]
            routeColor = RouteFields[keyName]["route_color"]
            WaitingTimeFields[keyName + k + "1"]= {"arrival":0, "destination":"","gpscoordinates":"","message":"","status":"not available", "stopName":stopName, "timestamp":"", "vehicle_type":routeType, "route_color":routeColor, "end_of_service":True, 'line':str(line_id) }
            WaitingTimeFields[keyName + k + "2"]= {"arrival":0, "destination":"","gpscoordinates":"","message":"","status":"not available", "stopName":stopName, "timestamp":"", "vehicle_type":routeType, "route_color":routeColor, "end_of_service":True, 'line':str(line_id) }

    q = " OR ".join(str(item) for item in stopIds)
    dataset='waiting-time-rt-production'
    StopData = getStibData(q, dataset)
    #pprint.pprint(StopData)
    if "records" in StopData:
        StopRecords = StopData['records']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            p = "1"
            x = 0
            eos = True
            av = "not available"
            pt = json.loads(r["fields"]["passingtimes"])
            pointId = r["fields"]["pointid"]
            lineId = "L" + str(r["fields"]["lineid"]) + "" + str(pointId) + p
            if lineId in WaitingTimeFields:
                destination = ""
                message = ""
                if "destination" in pt[x]:
                    destination = pt[x]["destination"][LANG]
                    eos = False
                    av = "available"
                if 'message' in pt[x]:
                    message  = pt[x]["message"][MESSAGE_LANG]
                t = pt[x]["expectedArrivalTime"]
                ttt = datetime.datetime.fromisoformat(t)
                tmp = pytz.utc.normalize(ttt)
                minutes = round( (tmp-now).total_seconds()/60)
                WaitingTimeFields[lineId]["line"] = str(r["fields"]["lineid"])
                WaitingTimeFields[lineId]["arrival"] = minutes
                WaitingTimeFields[lineId]["timestamp"] = t
                WaitingTimeFields[lineId]["message"] = message
                WaitingTimeFields[lineId]["end_of_service"] = eos
                WaitingTimeFields[lineId]["destination"] = destination
                WaitingTimeFields[lineId]["status"] = av
                WaitingTimeFields[lineId]["stopName"] = StopFields['STOP'+str(pointId)]["stop_names"][LANG]
                WaitingTimeFields[lineId]["gpscoordinates"] = StopFields['STOP'+str(pointId)]["gps_coordinates"]
                if len(pt) > 1:
                    p = "2"
                    x = 1
                    eos = True
                    pt = json.loads(r["fields"]["passingtimes"])
                    pointId = r["fields"]["pointid"]
                    lineId = "L" + str(r["fields"]["lineid"]) + "" + str(pointId) + p
                    message= ""
                    destination = ""
                    if "destination" in pt[x]:
                        destination = pt[x]["destination"][LANG]
                        eos = False
                        av = "available"
                    if 'message' in pt[x]:
                        message  = pt[x]["message"][MESSAGE_LANG]
                    t = pt[x]["expectedArrivalTime"]
                    ttt = datetime.datetime.fromisoformat(t)
                    tmp = pytz.utc.normalize(ttt)
                    minutes = round( (tmp-now).total_seconds()/60)
                    WaitingTimeFields[lineId]["line"] = str(r["fields"]["lineid"])
                    WaitingTimeFields[lineId]["arrival"] = minutes
                    WaitingTimeFields[lineId]["timestamp"] = t
                    WaitingTimeFields[lineId]["message"] = message
                    WaitingTimeFields[lineId]["destination"] = destination
                    WaitingTimeFields[lineId]["end_of_service"] = eos
                    WaitingTimeFields[lineId]["status"] = av
                    WaitingTimeFields[lineId]["stopName"] = StopFields['STOP'+str(pointId)]["stop_names"][LANG]
                    WaitingTimeFields[lineId]["gpscoordinates"] = StopFields['STOP'+str(pointId)]["gps_coordinates"]


    #pprint.pprint(WaitingTimeFields)
    return WaitingTimeFields




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

def publish(client):
    global counter
    msg_count = 0
    lastDate = datetime.datetime.today()
    data = getStopInfos()
    while True:
        msg = False
        if  checkUpdate(lastDate):
            data = getStopInfos()
            lastDate = datetime.datetime.today()

        msg = getWaitingTimes(data)
        if msg:
            for x in msg:
                 #  print(msg[x]["stopName"], ": ",msg[x]['line'],' ', msg[x]['arrival'],'min')
                 pprint.pprint(msg[x])
            msg = json.dumps(msg, indent=4, sort_keys=True, ensure_ascii=False)
            result = client.publish(mqtt_topic, msg)
            status = result[0]
            if status == 0:
                print(f"Send msg to topic `{mqtt_topic}`")
            else:
                print(f"Failed to send message to topic {mqtt_topic}")
            msg_count += 1
            print(msg_count)

        # result: [0, 1]

        print("sleep")
        time.sleep(20)

def checkUpdate(lastDate):
    today = datetime.datetime.today()
    one_week_ago = today - timedelta(days=7)
    if lastDate < one_week_ago:
        return True
    return False


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)




if __name__ == '__main__':

    run()
