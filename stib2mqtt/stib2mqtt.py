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
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import sqlite3


with open('config.yaml', 'r') as file:
    configuration = yaml.safe_load(file)

#STIB_API = "https://data.stib-mivb.be/api/records/1.0/search/"
STIB_API = "https://stibmivb.opendatasoft.com/api/explore/v2.1/catalog/datasets/"

STIB_API_KEY = configuration['stib_api_key']
mqtt_server = configuration['mqtt_server']

mqtt_port = configuration['mqtt_port']
mqtt_user = configuration['mqtt_user']
mqtt_password = configuration['mqtt_password']
mqtt_topic = configuration['mqtt_topic']
client_id = f'stib-mqtt-{random.randint(0, 1000)}'

STOPS = configuration['stops']
STS = configuration['test']
STOP_NAMES = configuration['stop_names']

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
    #print(q)    
    #print(dataset)    
    url = STIB_API + dataset + "/records"
    params = dict(
    where=q,
    start=0,
    rows=99,
    apikey = STIB_API_KEY
    )
    data = [] 
    try:
        r = requests.get(url=url, params=params)
        #print(r.request.url)
        data = r.json()
        #if "stop-details-production" in dataset:
            #print(data)
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

def getGtfsFiles():
    dataset = "gtfs-files-production"
    q = ""
    StopData = getStibData(q, dataset)
    if StopData and "results" in StopData:
        for r in StopData['results']:
            print(r['file']['filename'])
            response = requests.get(r['file']['url'])
            with open(r['file']['filename'], mode="wb") as file:
                file.write(response.content)
def updateDBTrips():
    data = pd.read_csv('trips.txt')
    trips = data.to_dict('records')
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()
    for trip in trips:
        did  = trip['id'] = None 
        route_id = trip['route_id']
        service_id = trip['service_id']
        trip_id = trip['trip_id']
        trip_headsign = trip["trip_headsign"]
        direction_id= trip['direction_id']
        block_id = trip["block_id"]
        shape_id = trip["shape_id"]
        trips_tp = [did,route_id,service_id,trip_id,trip_headsign,direction_id,block_id,shape_id]
        #fields = ', '.join('%s' for _ in len(trips_tp))
        result = cursor.execute("INSERT OR REPLACE INTO trips VALUES (NULL, ?, ?, ?, ?, ?, ?, ?);", (route_id, service_id, trip_id, trip_headsign, direction_id, block_id, shape_id,) )
        print(result)
    connection.commit()
    return False

def updateDBStopTimes():
    data = pd.read_csv('stop_times.txt')
    trips = data.to_dict('records')
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()
    #trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type   
    for trip in trips:
        trip_id = trip['trip_id']
        arrival_time = trip['arrival_time']
        departure_time = trip['departure_time']
        stop_id = trip["stop_id"]
        stop_sequence = trip['stop_sequence']
        pickup_type = trip["pickup_type"]
        drop_off_type = trip["drop_off_type"]
        result = cursor.execute("INSERT OR REPLACE INTO stop_times VALUES (NULL, ?, ?, ?, ?, ?, ?, ?);", (trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type) )
        print(result)
    connection.commit()
    return False

def updateDBRoutes():
    data = pd.read_csv('routes.txt')
    trips = data.to_dict('records')
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()
    #route_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
    for trip in trips:
        route_id = trip['route_id']
        route_short_name = trip['route_short_name']
        route_long_name= trip['route_long_name']
        route_desc= trip["route_desc"]
        route_type= trip['route_type']
        route_url= trip["route_url"]
        route_color= trip["route_color"]
        route_text_color= trip["route_text_color"]
        result = cursor.execute("INSERT OR REPLACE INTO routes VALUES (NULL, ?, ?, ?, ?, ?, ?, ?,?);", (route_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color) )
        print(result)
    connection.commit()
    return False

def updateDBStops():
    data = pd.read_csv('stops.txt')
    trips = data.to_dict('records')
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()
    #route_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color
    for trip in trips:
        stop_id = trip["stop_id"]
        stop_code = trip["stop_code"]
        stop_name = trip["stop_name"]
        stop_desc = trip["stop_desc"]
        stop_lat = trip["stop_lat"]
        stop_lon = trip["stop_lon"]
        zone_id = trip["zone_id"]
        stop_url = trip["stop_url"]
        location_type = trip["location_type"]
        parent_station = trip["parent_station"]
        result = cursor.execute("INSERT OR REPLACE INTO stops VALUES (NULL, ?, ?, ?, ?, ?, ?,?, ?, ?,?);", (stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station) )
        print(result)
    connection.commit()
    return False

def updateDBCalendar():
    service = "calendar"
    data = pd.read_csv(service + '.txt')
    trips = data.to_dict('records')
    connection = sqlite3.connect("db.sqlite")
    print(trips)
    cursor = connection.cursor()
    #service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date
    for trip in trips:
        service_id = trip["service_id"] 
        monday = trip["monday"] 
        tuesday = trip["tuesday"] 
        wednesday = trip["wednesday"] 
        thursday = trip["thursday"] 
        friday = trip["friday"] 
        saturday = trip["saturday"] 
        sunday = trip["sunday"] 
        start_date = trip["start_date"] 
        end_date = trip["end_date"] 
        result = cursor.execute("INSERT OR REPLACE INTO calendar VALUES ( ?, ?, ?, ?, ?, ?,?, ?, ?,?);", (service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date) )
        print(result)
    connection.commit()
    return False



def getStopInfos():
    #getGtfsFiles()
    #updateDB() 
    #updateDBStopTimes() 
    #updateDBRoutes() 
    #updateDBStops() 
    #updateDBCalendar()
    dataset='stop-details-production'
    stop_names = []
    for stop, stop_name in enumerate(STOP_NAMES):
        stop_names.append(stop_name)
    q = " OR ".join(' name like "' + item + '"' for item in stop_names)
    StopData = getStibData(q, dataset)
    stopIds = []
    lineIds = []
    StopFields = {}
    LineFields = {}
    RouteFields = {}

    if StopData and "results" in StopData:
        StopRecords = StopData['results']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            stopsName  = json.loads(r["name"])
            stopId = r["id"]
            stopIds.append(stopId)
            gpscoordinates = json.loads(r['gpscoordinates']) 
            k = ''.join(i for i in str(stopId) if i.isdigit())
            k = "STOP" + k
            StopFields[k] = {"stop_id" : stopId, "stop_names" : stopsName, "gps_coordinates" : gpscoordinates }

    return {"stops": StopFields, "lines": LineFields, "routes": RouteFields, 'stopids' : stopIds}    
    return (StopFields)
    # get line numbers by stop

 
def getStopInfosOld():
    stopIds = []
    lineIds = []
    StopFields = {}
    LineFields = {}
    RouteFields = {}
    for stopId, stop in enumerate(STOPS):
        stopIds.append(stop['stop_id'])
        k = ''.join(i for i in str(stop['stop_id']) if i.isdigit())
        k = "STOP" +  k
        StopFields[k] = {"stop_id":stop['stop_id']}
        for line_id in stop['line_numbers']:
            if line_id not in lineIds:
                keyName= "L" + str(line_id) 
                LineFields[keyName]={"line_id":line_id}
                RouteFields[keyName]={"line_id":line_id}
                lineIds.append(line_id)

    q = " OR ".join(' id = "' + str(item) + '"' for item in stopIds)
    dataset='stop-details-production'
    StopData = getStibData(q, dataset)
    if StopData and "results" in StopData:
        StopRecords = StopData['results']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            stopsName  = json.loads(r["name"])
            #print("STOPSNAME " +  r["name"] + " " + r["id"])
            stopId = r["id"]
            gpscoordinates = json.loads(r['gpscoordinates']) 
            k = "STOP" + str(stopId)
            StopFields[k]["stop_names"] = stopsName
            StopFields[k]["gps_coordinates"] = gpscoordinates 
    else: return []
            
    q = " OR ".join('lineid = "' + str(item) + '"' for item in lineIds)
    dataset='stops-by-line-production'
    StopData = getStibData(q, dataset)
    if "results" in StopData:
        StopRecords = StopData['results']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            lineId  = r["lineid"]
            k = "L" +  str(lineId)
            if k in LineFields:
                destination = (r["destination"])
                direction = (r["direction"])
                LineFields[k]["destination"] = destination
                LineFields[k]["direction"] = direction 

    q = " OR ".join('route_short_name = "' + str(item) + '"' for item in lineIds)
    dataset='gtfs-routes-production'
    RoutesData = getStibData(q, dataset)
    if "results" in RoutesData:  
        RoutesRecords = RoutesData['results']
    else:
        RoutesRecords = False
    if RoutesRecords:
        for r in RoutesRecords:
            lineId  = r["route_short_name"]
            k = "L" +  str(lineId)
            if k in LineFields:
                RouteFields[k]["route_type"] = r['route_type']
                RouteFields[k]["route_color"] = r['route_color']
        
    return {"stops": StopFields, "lines": LineFields, "routes": RouteFields, "lineIds" : lineIds}    


def getWaitingTimes(fields):
    StopFields = fields['stops']
    StopIds = fields["stopids"]
    #q = " OR ".join(' pointid like "' + item + '"' for item in StopIds)
    q = " OR ".join('pointid like "' + ''.join(i for i in str(item) if i.isdigit()) + '"' for item in StopIds)
    dataset='waiting-time-rt-production'
    StopData = getStibData(q, dataset)
    results = StopData['results']
    #pprint.pprint(results)
    lineIds = []
    WaitingTimeFields = {}
    for r in results:
        k = "STOP" + str(r['pointid'])
        l = "L" + r['lineid']       
        passingtimes = json.loads(r['passingtimes'])
        for pt in passingtimes:
            t = pt["expectedArrivalTime"]
            now = pytz.utc.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
            ttt = datetime.datetime.fromisoformat(t)
            tmp = pytz.utc.normalize(ttt)
            minutes = round( (tmp-now).total_seconds()/60)
            pt['minutes'] = minutes
            if "pt" in StopFields[k]:
                StopFields[k]["pt"].append(pt)
            else:
                StopFields[k]["pt"] =  [pt] 

        if r['lineid'] not in lineIds:
            lineIds.append(r['lineid'])
    #pprint.pprint(StopFields)
    q = " OR ".join(' route_short_name like "' + item + '"' for item in lineIds)
    dataset='gtfs-routes-production'
    StopData = getStibData(q, dataset)
    results = StopData['results']
    LineFields = {}
    for r in results:
        LineFields[r['route_short_name']] = {"route_short_name" : r["route_short_name"], "route_long_name" :  r['route_long_name'], "route_type" : r['route_type'], "route_color" : r['route_color'] }
    #pprint.pprint(LineFields)
    for stop, r  in StopFields.items():
        if "pt" in r:
            for pt in r['pt']:
                if pt['lineId'] in LineFields:
                    pt['route'] = LineFields[pt['lineId']]
                else:
                    print('no route for ' + pt['lineId'])
    #pprint.pprint(StopFields)
    return StopFields

def getWaitingTimesOld(fields):
    #print(fields)
    if (len(fields) == 0 ):
        return False
    now = pytz.utc.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    stopIds = []
    lineIds = []
    StopFields = fields['stops']
    LineFields = fields['lines'] 
    RouteFields = fields['routes']
    WaitingTimeFields = {}
    for stopId, stop in enumerate(STOPS):
        stopIds.append(stop['stop_id'])
        k = str(stop['stop_id']) 
        for line_id in stop['line_numbers']:
            keyName= "L" + str(line_id) 
            if "stop_names" in StopFields['STOP' + k]:
                stopName = StopFields['STOP' + k]['stop_names'][LANG]
            else:
                stopName = "UNKNOWN"
                
            routeType = RouteFields[keyName]["route_type"]
            routeColor = RouteFields[keyName]["route_color"]
            WaitingTimeFields[keyName + k + "1"]= { "p":"1","arrival":0, "destination":"","gpscoordinates":"","message":"","status":"not available", "stopName":stopName, "timestamp":"", "vehicle_type":routeType, "route_color":routeColor, "end_of_service":True, 'line':str(line_id) }
            WaitingTimeFields[keyName + k + "2"]= {"p":"2","arrival":0, "destination":"","gpscoordinates":"","message":"","status":"not available", "stopName":stopName, "timestamp":"", "vehicle_type":routeType, "route_color":routeColor, "end_of_service":True, 'line':str(line_id) }

    q = " OR ".join('pointid = "' + ''.join(i for i in str(item) if i.isdigit()) + '"' for item in stopIds)
    dataset='waiting-time-rt-production'
    StopData = getStibData(q, dataset)
    #pprint.pprint(StopData)
    if "results" in StopData:
        StopRecords = StopData['results']
    else:
        StopRecords = False
    if StopRecords:
        for r in StopRecords:
            p = "1"
            x = 0
            eos = True
            av = "not available"
            pt = json.loads(r["passingtimes"])
            pointId = r["pointid"]
            lineId = "L" + str(r["lineid"]) + "" + str(pointId) + p
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
                WaitingTimeFields[lineId]["line"] = str(r["lineid"])
                WaitingTimeFields[lineId]["arrival"] = minutes
                WaitingTimeFields[lineId]["p"] = p 
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
                    pt = json.loads(r["passingtimes"])
                    pointId = r["pointid"]
                    lineId = "L" + str(r["lineid"]) + "" + str(pointId) + p
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
                    WaitingTimeFields[lineId]["line"] = str(r["lineid"])
                    WaitingTimeFields[lineId]["arrival"] = minutes
                    WaitingTimeFields[lineId]["timestamp"] = t 
                    WaitingTimeFields[lineId]["message"] = message 
                    WaitingTimeFields[lineId]["destination"] = destination
                    WaitingTimeFields[lineId]["end_of_service"] = eos 
                    WaitingTimeFields[lineId]["status"] = av 
                    WaitingTimeFields[lineId]["p"] = p 
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
        msg_data = {}
        msg = getWaitingTimes(data)
        if msg:
            #msg = json.dumps(msg, indent=4, sort_keys=True, ensure_ascii=False)
            for i, x in msg.items():
                 #  print(msg[x]["stopName"], ": ",msg[x]['line'],' ', msg[x]['arrival'],'min')
                 stopname = (x['stop_names'][LANG])
                 c = []
                 if "pt" in x:
                     for pt in x["pt"]:
                        p = ""
                        lineId = pt["lineId"]
                        ob = "L" + lineId + x['stop_id']  
                        if ob not in c:
                            object_id = ob + str(1)
                            c.append(ob)
                            p = str(1)
                        else:
                            object_id = ob + str(2)
                            p = str(2)
                            c.remove(ob)

                        arrival = pt['minutes']
                        destination = "INCONNUE"
                        if "destination" in pt:
                            destination = pt['destination'][LANG]
                        message = ""
                        if "message" in pt:
                            message = pt['message']
                        color = "UNKNOWN"
                        route_type = "UNKNOWN"
                        route_long_name = "UNKNOWN"
                        if "route" in pt:
                            color  = pt['route']['route_color']
                            route_type  = pt['route']['route_type']
                            route_long_name = pt['route']['route_long_name'] 
                        eat = "UNKNOWN"
                        eos = True
                        if 'expectedArrivalTime' in pt:
                            eat = pt['expectedArrivalTime']
                            eos = False
                        msg_data[object_id] = {"arrival" : arrival, 
                                "color": color, 
                                "type": route_type, 
                                "route": route_long_name, 
                                "destination": destination,
                                "StopName" : stopname,
                                "message" : message,
                                "timestamp" : eat,
                                "end_of_servie" : eos }
                 
                        topic = "homeassistant/sensor/stib" + object_id 
                        config = topic+"/config"
                        state = topic+"/state"
                        mconfig = {}
                        mconfig["device_class"] = "duration"
                        mconfig["icon"] = "mdi:"+ msg_data[object_id]['type'].lower()
                        mconfig["state_topic"] =  state
                        #mconfig["state_class"] = "measurement"
                        mconfig["unit_of_measurement"] = "min"
                        mconfig["value_template"] =  "{{value_json.arrival}}"
                        mconfig["unique_id"] =  "stib" + object_id
                        mconfig["json_attributes_topic"] = "stib" 
                        mconfig["json_attributes_template"] = "{{value_json." + object_id  + " | default('') | to_json}}"
                        i = {}
                        i['identifiers']=  ["stib"+ object_id ]
                        i['name'] = "STIB " + stopname + " (" + x['stop_id'] + ") " + msg_data[object_id]['type'] + " " + lineId  + " " + str(p)
                        mconfig["device"] = i
                        jconfig = json.dumps(mconfig, indent=4, sort_keys=True, ensure_ascii=False)
                        mstate = { "arrival" :arrival }
                        jstate = json.dumps(mstate, indent=4, sort_keys=True, ensure_ascii=False)
                        print(jconfig)
                        #pprint.pprint(jconfig)
                        jconfig = ""
                        jstate = ""
                        
                        client.publish(config, jconfig, qos=0, retain=True)
                        client.publish(state, jstate, qos=0, retain=False)
                                               
                        
            #pprint.pprint(msg_data)
            msg = json.dumps(msg_data, indent=4, sort_keys=True, ensure_ascii=False)
            """ result = client.publish(mqtt_topic, msg)
            status = result[0]
            if status == 0:
                print(f"Send msg to topic `{mqtt_topic}`")
            else:
                print(f"Failed to send message to topic {mqtt_topic}") """
            msg_count += 1
            print(msg_count)
            break

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
