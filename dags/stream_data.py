from kafka import KafkaProducer
import requests as rq
import logging
import time
import json


def extract_data():
    url = 'https://randomuser.me/api/'
    response = rq.get(url)
    
    if response.status_code != 200:
        raise Exception(f"L'API {url} ne fonctionne pas.\n Code: {response.status_code}")

    return response.json()['results'][0]

def format_data(response):
    location = response['location']
    data = {
        "gender": response['gender'],
        "first_name": response['name']['first'],
        "last_name": response['name']['last'],
        "location": f"{location['street']['number']} {location['street']['name']} street",
        "city": location['city'],
        "state": location['state'],
        "country": location['country'],
        "postcode": location['postcode'],
        "email": response['email'],
        "birth_date": response['dob']['date'],
        "age": response['dob']['age'],
        "phone": response['phone'],
        "picture": response['picture']['medium']
    }
    
    return data

def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()
    
    while True:
        if time.time() > current_time + 60:
            break
        try:
            response = extract_data()
            data = format_data(response)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as error:
            logging.error(f"Une erreur est survenue: {error}")
            continue
