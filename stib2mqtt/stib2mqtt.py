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
import yaml

with open('config.yml', 'r') as file:
    configuration = yaml.safe_load(file)

STIB_API = "https://data.stib-mivb.be/api/records/1.0/search/"

STIB_API_KEY = configuration['stib_api_key']
mqtt_server = configuration['mqtt_server']

mqtt_port = configuration['mqtt_port']
mqtt_user = configuration['mqtt_user']
mqtt_password = configuration['mqtt_password']
client_id = f'stib-mqtt-{random.randint(0, 1000)}'

STOPS = configuration['stops']

def getStibData(q, dataset):
    url = STIB_API
    params = dict(
    dataset = dataset,
    q=q,
    start=0,
    rows=99,
    apikey = STIB_API_KEY
    )
    data = False
    try:
        r = requests.get(url=url, params=params)
        r.raise_for_status()
        data = r.json()
    except requests.exceptions.HTTPError as e:
        print (e.response.text)
    return data

def getStopInfos():
    #create query
    for stopId, stop in enumerate(STOPS):
        print(index, stop)
    return False    

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
    client.connect(mqtt_server, mqtt_port)
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




if __name__ == '__main__':
    getStopInfos()
#    run()