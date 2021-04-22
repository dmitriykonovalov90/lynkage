import requests
import json


def get_token(username, password, client_secret, grant_type, client_id, organization):
    body = {
        "username": username,
        "password": password,
        "client_secret": client_secret,
        "grant_type": grant_type,
        "client_id": client_id,
        "organization": organization
           }
    response = requests.post('https://api.test.navigator.lynkage.ru/oauth/token', data=body)
    assert response.status_code != 500, "internal server error"
    requestdict = json.loads(response.content)
    headers = {"Authorization": 'Bearer ' + requestdict['access_token']}
    return headers
