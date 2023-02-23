import requests

session = requests.Session()

base_url = 'https://data.stib-mivb.be/api/records/1.0/search/'

headers = {'user-agent': 'pyStibHa (daniel.nix@gmail.com)'}
