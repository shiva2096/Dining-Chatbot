from variables import * 
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch

import boto3
import requests
import json

host = 'https://search-concierge-chatbot-mupjitn6btj57fg6oxgajiffdy.us-east-1.es.amazonaws.com/' 
path = 'restaurants/Restaurant/' 
region = 'us-east-1' 
service = 'es'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
yelp_restaurants_table = dynamodb.Table(
    'yelp-restaurants')

index = 1

lastEvaluatedKey = None
table_items = [] # Result Array

while True:

    if lastEvaluatedKey == None:
        response = yelp_restaurants_table.scan() # This only runs the first time - provide no ExclusiveStartKey initially
    else:
        response = yelp_restaurants_table.scan(
        ExclusiveStartKey=lastEvaluatedKey # In subsequent calls, provide the ExclusiveStartKey
    )

    table_items.extend(response['Items']) # Appending to our resultset list

    # Set our lastEvlauatedKey to the value for next operation,
    # else, there's no more results and we can exit
    if 'LastEvaluatedKey' in response:
        lastEvaluatedKey = response['LastEvaluatedKey']
    else:
        break

print(len(table_items)) # Return Value: 6


for item in table_items:
    # print(item['Name'])
    id = item['id']
    cuisine = item['Cuisine']

    url = host + path + id
    payload = {'RestaurantID': id, "Cuisine": cuisine}
    response = requests.post(url, auth=("demo", "Demo@1234"), json=payload)

    print("\n{}: {} - {}".format(index, cuisine, response.text))
    index = index + 1


headers = {'content-type': 'application/json'}
esUrl = 'https://search-concierge-chatbot-mupjitn6btj57fg6oxgajiffdy.us-east-1.es.amazonaws.com/_search?q={cuisine}'.format(cuisine = cuisine)
esResponse = requests.get(esUrl, auth=("demo", "Demo@1234"), data=json.dumps(payload), headers=headers)

print(esResponse.text)