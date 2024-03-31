from kafka import KafkaProducer
import requests as rq
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
    response = extract_data()
    data = format_data(response)
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    
    producer.send('users_created', json.dumps(data).encode('utf-8'))
    
    # return data
    # print(json.dumps(data, indent=4))

stream_data()