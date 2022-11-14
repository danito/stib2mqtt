
import json
import requests
import datetime
from datetime import timedelta
from datetime import timezone
from dateutil import parser
import pytz
from paho.mqtt import client as mqtt_client
import random
import time

broker = 'localhost'
port = 1883
topic = "stib"
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'x'
password = ''
stopIds = [5719,1015, 1085]
toDay = datetime.date.today().day

#https://data.stib-mivb.be/api/records/1.0/search/?dataset=waiting-time-rt-production&q=lineid%3D54
#print(records)
counter = 0
def getJson(q):
    global counter
    #print("getJson")
    url = "https://data.stib-mivb.be/api/records/1.0/search/"
    params = dict(
    dataset='waiting-time-rt-production',
    q=q,
    start=0,
    rows=99,
    apikey = ''
    )

    resp = requests.get(url=url, params=params)
    data = resp.json()
    print(data)
    if "records" in data:
        counter += 1
        records = data['records']
    else:
        print(f"getJson {data}")
        records = False
    return records

def getStopsByLine(q = "54"):
    global counter
    print("getJson")
    url = "https://stibmivb.opendatasoft.com/api/records/1.0/search/"
    params = dict(
    dataset='stops-by-line-production',
    q=q,
    start=0,
    rows=99,
    apikey = ''
    )

    resp = requests.get(url=url, params=params)
    data = resp.json()
    print(data)
    if "records" in data:
        counter += 1
        records = data['records']
    else:
        print(f"getStopsJson {data}")
        records = False
    return records

def getStopInfo(q):
    global counter
    print("getStopInfo")
    url = "https://stibmivb.opendatasoft.com/api/records/1.0/search/"
    params = dict(
    dataset='stop-details-production',
    q=q,
    start=0,
    rows=99,
    apikey = ''
    )

    resp = requests.get(url=url, params=params)
    counter += 1
    data = resp.json()
    print(data)
    if "records" in data:
        counter += 1
        records = data['records']
    else:
        print(data)
        records = False
    return records


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client):
    global counter
    msg_count = 0
    while True:
        msg = {}
        msg = getMinutes()
        if msg:
            msg = json.dumps(msg, indent=4, sort_keys=True, ensure_ascii=False)
            result = client.publish(topic, msg)
            status = result[0]
            if status == 0:
                print(f"Send `{msg}` to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")
            msg_count += 1
            print(msg_count)
        print(f"Counter {counter}")
        if toDay != datetime.date.today().day:
            counter = 0
            today =  datetime.date.today().day

        # result: [0, 1]
        
        print("sleep")
        time.sleep(20)


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

def setStopInfo():
    stop = " OR ".join(str(item) for item in stopIds)
    stopInfos = getStopInfo(stop)
    if not stopInfos:
        return False
    names = {}
    for s in stopInfos:
        stopsName  = json.loads(s["fields"]["name"])
        stopId = "L" + str(s["fields"]["id"])
        gpscoordinates = json.loads(s["fields"]['gpscoordinates']) 
        names[stopId] = {"names": stopsName, "gpscoordinates" :  gpscoordinates}
    print(names)
    return names


stopNames = setStopInfo()

def getMinutes():
    global stopNames
    stop = " OR ".join(str(item) for item in stopIds)
    if not stopNames:
        stopNames = setStopInfo()
        return False
    records = getJson(stop)
    now = pytz.utc.normalize(pytz.utc.localize(datetime.datetime.utcnow()))
    data = {}
    for i, r in enumerate(records):
        x = 0
        pt = json.loads(r["fields"]["passingtimes"])
        ts = r["record_timestamp"]
        print("PT")
        print(pt)
        pointId = r["fields"]["pointid"]
        lineId = "L" + str(r["fields"]["lineid"]) + "" + str(pointId) + "1"
        if "destination" in pt[x]:
            destination = pt[0]["destination"]["fr"]
        else:
            destination = "Fin Service"
        message = "";
        if 'message' in pt[x]:
            message  = pt[x]["message"]["fr"]
        t = pt[0]["expectedArrivalTime"]
        ttt = datetime.datetime.fromisoformat(t)
        tmp = pytz.utc.normalize(ttt)
        minutes = round( (tmp-now).total_seconds()/60)
        data[lineId] = {}
        data[lineId]["arrival"] = minutes
        data[lineId]["timestamp"] = t
        data[lineId]["message"] = message
        data[lineId]["destination"] = destination
        data[lineId]["stopName"] = stopNames["L" + str(pointId)]["names"]["fr"] 
        data[lineId]["gpscoordinates"] = str(stopNames["L" + str(pointId)]["gpscoordinates"]["latitude"]) + ", " +str(stopNames["L" + str(pointId)]["gpscoordinates"]["longitude"])
        data[lineId]["status"] = "available"
        if minutes < 0:
            data[lineId]["status"] = "not available"
        lineId = "L"+str(r["fields"]["lineid"]) + "" + str(pointId) +  "2"
        destination = ""      
        minutes = ""
        message = "";
        data[lineId] = {}
        data[lineId]["arrival"] = minutes
        data[lineId]["timestamp"] = "" 
        data[lineId]["destination"] = destination
        data[lineId]["message"] = "" 
        data[lineId]["status"] = "not available"
        if len(pt) > 0:
            x = 1
        print(pt)
        if "destination" in pt[x]:
            destination = pt[x]["destination"]["fr"]
        else:
            destination = "Fin Service"

        if 'message' in pt[x]:
            message  = pt[x]["message"]["fr"]
        t = pt[x]["expectedArrivalTime"]
        ttt = datetime.datetime.fromisoformat(t)
        tmp = pytz.utc.normalize(ttt)
        minutes = round( (tmp-now).total_seconds()/60)
        if destination == 'Fin Service':
            minutes = -999
        data[lineId] = {}
        data[lineId]["arrival"] = minutes
        data[lineId]["timestamp"] = t
        data[lineId]["destination"] = destination
        data[lineId]["message"] = message
        data[lineId]["status"] = "available"
        data[lineId]["stopName"] = stopNames["L" + str(pointId)]["names"]["fr"] 
        data[lineId]["gpscoordinates"] = str(stopNames["L" + str(pointId)]["gpscoordinates"]["latitude"]) + ", " +str(stopNames["L" + str(pointId)]["gpscoordinates"]["longitude"])
        if minutes < 0:
            data[lineId]["status"] = "not available"
#return json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False)
    return data




if __name__ == '__main__':
    run()

#print(json.loads(records[0]["fields"]["passingtimes"])[0]["lineId"])
#print(records[0]["fields"]["passingtimes"])

