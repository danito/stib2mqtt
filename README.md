# stib2mqtt
Send realtime data from the Stib api v2.1 to a mqtt broker.

Get your API key from https://stibmivb.opendatasoft.com/pages/home/

# MQTT server settings
Edit config.yaml with your settings for your mqtt broker.

## Info

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
stop_names:
  - 'SAINT DENIS'
  - 'MAX WALLER'
  - 'FOREST CENTRE'
```
This will publish config, attributes and state to mqtt topic homeassistant/sensor/stibLxxStopIdX/ and will be autodiscovered.

