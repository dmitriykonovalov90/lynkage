import requests
import json


def get_token():
    body = {
        "username": "ta.yakobson1",
        "password": "iWAQtZMw",
        "client_secret": "23IzWSgkX5MUlpxSAYJr2o1sM8DRkLXI7vlZFExW",
        "grant_type": "password",
        "client_id": 2,
        "organization": "lcentrix"
           }
    response = requests.post('http://localhost/login', data=body)
    assert response.status_code != 500, "Необработанная ошибка от Laravel!"
    requestdict = json.loads(response.content)
    headers = {"Authorization": 'Bearer ' + requestdict['data']['access_token']}
    return headers