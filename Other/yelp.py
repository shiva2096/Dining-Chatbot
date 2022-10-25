import logging
import os
import boto3
import json
import requests
import time
import datetime
import traceback

from decimal import Decimal

def yelp():
    
    api_key = "4bDFA5yKFIS-Oas0ud56n791zryYwPomGQVjAaWLcMbNLKtwSV2lF6TzI9laPPL7wZnwwGc0Rx7Qrh7HIEIg0BhBNukJ-3J3QAQ_lluLKGWWRqW5gC8SC1_Vn0gdYnYx"

    count = 0
    repeated = 0
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    yelp_restaurants_table = dynamodb.Table(
        'yelp-restaurants')

    headers = {'Authorization': 'Bearer {}'.format(api_key)}
    search_api_url = 'https://api.yelp.com/v3/businesses/search'

    # cities = {'New York', 'Boston'}
    cities = {'New York'}
    cuisines = {'indian','chinese','mexican','italian','thai','american','caribbean','korean'}

    try:
        for city in cities:
            for cuisine in cuisines:
                count = 0
                repeated = 0
                for i in range(0,1000,50):
                    params = {'term': cuisine + " restaurants", 
                        'location': city,
                        'offset' : i,
                        'limit': 50}
                    
                    response = requests.get(search_api_url, headers=headers, params=params, timeout=5)
                    data_dict = response.json(parse_float=Decimal)

                    # print(data_dict['businesses'][0])


                    for business in data_dict['businesses']:

                        result = ""
                        
                        id = business['id']
                        result = yelp_restaurants_table.get_item(
                            Key={
                                'id': id
                            }
                        )

                        if 'Item' in result:
                            repeated = repeated + 1
                        else:
                            count = count + 1

                        display_address = business['location']['display_address']
                        address_string = ' '.join(display_address)

                        item = {
                            'id': business['id'],
                            'Business ID': business['id'],
                            'insertedAtTimestamp': Decimal(time.time()),
                            'Name': business['name'],
                            'Cuisine': cuisine,
                            'Rating': business['rating'],
                            'Number of Reviews': business['review_count'],
                            'Address': address_string,
                            'Zip Code': business['location']['zip_code'],
                            'Latitude': str(business['coordinates']['latitude']),
                            'Longitude': str(business['coordinates']['longitude']),
                        }

                        dynamodb_response = yelp_restaurants_table.put_item(Item=item)

                print("\nCuisine: {} - Added: {}, Repeated: {}".format(cuisine, count, repeated))

    except Exception as e:
        print(data_dict)

yelp()