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

stib_api = "https://data.stib-mivb.be/api/records/1.0/search/"

stib_api_key = configuration['stib_api_key']
mqtt_server = configuration['mqtt_server']

mqtt_port = configuration['mqtt_port']
mqtt_user = configuration['mqtt_user']
mqtt_password = configuration['mqtt_password']

