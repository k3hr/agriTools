import requests

url = 'https://www.data.gouv.fr/api/1/datasets/cotations-du-reseau-des-nouvelles-des-marches/'
resp = requests.get(url, timeout=10)
resp.raise_for_status()
data = resp.json()
print('dataset_keys=', list(data.keys()))
resources = data.get('resources', [])
print('resources count', len(resources))
for idx, r in enumerate(resources[:5], 1):
    print(f'RESOURCE {idx}')
    for k in ('id','format','title','url','description','download_url','path','name','relations','created_at','updated_at'):
        if k in r:
            print(f'  {k}: {r[k]}')
