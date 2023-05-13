import requests

url = 'https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD'
local_filename = 'chicago-data.csv'

with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(local_filename, 'wb') as f:
        total_size = int(r.headers.get('content-length', 0))
        downloaded_size = 0
        chunk_size = 8192
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded_size += len(chunk)
                if total_size > 0:
                    progress = downloaded_size / total_size * 100
                    print(f'{downloaded_size}/{total_size} bytes downloaded ({progress:.2f}%)', end='\r')
    print('Download complete.')
