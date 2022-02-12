import requests

def download_raw_data():
    url = "https://opendata.paris.fr/api/v2"
    dataset_name = "/catalog/datasets/comptages-routiers-permanents/exports/csv?limit=-1&offset=0&"
    where = "refine=t_1h%3A%272021-12-13%27&timezone=UTC"

    r = requests.get(url+dataset_name+where, allow_redirects=True)
    open('data.csv', 'wb').write(r.content)


download_raw_data()
