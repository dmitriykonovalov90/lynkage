import requests
import json


def get_token():
    body = {
        "username": "o.filippova",
        "password": "KkpGD4Yg",
        "client_secret": "23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW",
        "grant_type": "password",
        "client_id": 2,
        "organization": "rtk"
           }
    response = requests.post('https://api.test.navigator.lynkage.ru/oauth/token', data=body)
    assert response.status_code != 500, "Необработанная ошибка от Laravel!"
    requestdict = json.loads(response.content)
    headers = {"Authorization": 'Bearer ' + requestdict['access_token']}
    return headers
