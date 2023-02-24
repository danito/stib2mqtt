import requests

session = requests.Session()

base_url = 'https://data.stib-mivb.be/api/records/1.0/search/'

headers = {'user-agent': 'pyStibHa (daniel.nix@gmail.com)'}

datasets  = { 'stop-details-production' : [],
             'stops-by-line-production' : [],
             'gtfs-routes-production' : [],
             'waiting-time-rt-production' : [],
              }

class stib:

    def __init__(self,apikey=None):
        self.apikey = apikey

            
   
    def do_request(self, dataset, q):
        if dataset in datasets:
            url = base_url
            params = {
                'dataset' : dataset,
                'q': q,
                'start' : 0,
                'rows' : 99,
                'apikey' : self.apikey
            }
            
            try:
                response = session.get(url, params=params, headers=headers)
                try:
                    json_data = response.json()
                    return json_data
                except ValueError:
                    return -1
            except requests.exceptions.RequestException as e:
                print(e)
                try:
                    session.get('https://1.1.1.1/', timeout=1)
                except requests.exceptions.ConnectionError:
                    print("Your internet connection doesn't seem to be working.")
                    return -1
                else:
                    print("The StibMivb API doesn't seem to be working.")
                    return -1

    def getStopDetails(self, stops=None):
        if stops:
            stopIds = list(stops.keys())
            q = " OR ".join(str(item) for item in stopIds)
            dataset='stop-details-production'
            json_data = self.do_request(dataset, q)
            return json_data

    def getStopsByLine(self, stops=None):
        if stops:
            lineIds = list(stops.values())
            lineIds = list(dict.fromkeys(lineIds))
            q = " OR ".join(str(item) for item in lineIds)
            dataset='stops-by-line-production'
            json_data = self.do_request(dataset, q)
            return json_data

    def getGtfsRoutes(self, stops=None):
        if stops:
            dataset='gtfs-routes-production'
            lineIds = list(stops.values())
            lineIds = list(dict.fromkeys(stops))
            q = " OR ".join(str(item) for item in lineIds)
            json_data = self.do_request(dataset, q)
            return json_data

    def getWaitingTimes(self, stops=None):
        if stops:
            dataset='waiting-time-rt-production'
            stopIds = list(stops.keys())
            q = " OR ".join(str(item) for item in stopIds)
            dataset='stop-details-production'
            json_data = self.do_request(dataset, q)
            return json_data

    def getRealTimeData(self, stops=None, update=False):
        if stops:
            stopIds = list(stops.keys())
            lineIds = list(stops.values())
            lineIds = list(dict.fromkeys(stops))
            data = []
            if update:


