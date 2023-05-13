import requests

url = 'https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv'
local_filename = 'newyork.csv'

response = requests.get(url, stream=True)
response.raise_for_status()

with open(local_filename, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)
~                           
