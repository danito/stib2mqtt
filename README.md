# stib2mqtt
Send realtime data from the Stib api v2 to a mqtt broker.


Get your API key from https://data.stib-mivb.be/account/api-keys/

# MQTT server settings
Edit config.yml with your settings for your mqtt broker.

## Info
### How to get the stop ids line numbers?

Go to http://www.stib-mivb.be/horaires-dienstregeling2.html, select the line, the destination and then the stop name.
The stop id can be found at the end of the url after `_stop=`.

### example of config.yml
```yaml
mqtt_server: 'my.mqttser.er'
mqtt_port: '1883'
mqtt_user: 'username'
mqtt_password: 'password'
mqtt_topic: 'stib'

stib_api_key: 'a1b2c3d4e5f6g7h8'
lang: 'fr' # fr or nl
message_lang: 'fr' # fr, nl, or en

#https://www.stib-mivb.be/horaires-dienstregeling2.html?l=fr&_line=T82&_directioncode=V&_stop=3194
stops:
  - stop_id: 3194
    line_numbers:
      - 50
      - T82
  - stop_id: 5219
    line_numbers:
      - 54
      - 97
      - 74
```
Output will be a json string with 2 results for each line and stop.
Arrival: the minutes until next real time arrival time
Destination: Name of the destination stop
End_of_service: true if traffic stopped
gpscoordinates: gps coordinates of the stop
message: Service message
stopName: stop name
timestamp: the timestamp of the arrival time
vehicle_type: not used yet

### example of output
```json
{
    "LT8231941": {
        "arrival": 1,
        "destination": "GARE DE BERCHEM",
        "end_of_service": false,
        "gpscoordinates": {
            "latitude": 50.809427,
            "longitude": 4.313018
        },
        "message": "",
        "status": "available",
        "stopName": "SAINT-DENIS",
        "timestamp": "2022-11-16T12:07:00+01:00",
        "vehicle_type": ""
    },
    "LT8231942": {
        "arrival": 12,
        "destination": "GARE DE BERCHEM",
        "end_of_service": false,
        "gpscoordinates": {
            "latitude": 50.809427,
            "longitude": 4.313018
        },
        "message": "",
        "status": "available",
        "stopName": "SAINT-DENIS",
        "timestamp": "2022-11-16T12:18:00+01:00",
        "vehicle_type": ""
    },
    "LT8231961": {
        "arrival": 4,
        "destination": "DROGENBOS",
        "end_of_service": false,
        "gpscoordinates": {
            "latitude": 50.809701,
            "longitude": 4.313231
        },
        "message": "",
        "status": "available",
        "stopName": "SAINT-DENIS",
        "timestamp": "2022-11-16T12:10:00+01:00",
        "vehicle_type": ""
    },
    "LT8231962": {
        "arrival": 7,
        "destination": "DROGENBOS",
        "end_of_service": false,
        "gpscoordinates": {
            "latitude": 50.809701,
            "longitude": 4.313231
        },
        "message": "",
        "status": "available",
        "stopName": "SAINT-DENIS",
        "timestamp": "2022-11-16T12:13:00+01:00",
        "vehicle_type": ""
    }
```
### home-assistant
Exaple of Home-Assistant integration.


```yaml
sensor:
  - platform: mqtt
    name: "T82 dir Gare du Midi 1er passage Audi"
    state_topic: stib
    value_template: "{{ value_json.LT8231961.arrival }}"
    unit_of_measurement: "min"
    state_class: "measurement"
    json_attributes_topic: "stib"
    json_attributes_template: "{{ value_json.LT8231962 | default('') | tojson}}"    
  - platform: mqtt
    name: "T82 dir Gare du Midi 2e passage Audi"
    state_topic: stib
    value_template: "{{ value_json.L5031962.arrival }}"
    unit_of_measurement: "min"
    state_class: "measurement"
    json_attributes_topic: "stib"
    json_attributes_template: "{{ value_json.LT8231962 | default('') | tojson}}"
```
