import boto3
import json
import logging
import random
from boto3.dynamodb.conditions import Key, Attr
from botocore.vendored import requests
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def getSQSMsg():
    SQS = boto3.client("sqs")
    url = 'https://sqs.us-east-1.amazonaws.com/571251177522/DiningBotQueue'
    response = SQS.receive_message(
        QueueUrl=url,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    try:
        message = response['Messages'][0]
        if message is None:
            logger.debug("Empty message")
            return None
    except KeyError:
        logger.debug("No message in the queue")
        return None
    message = response['Messages'][0]
    SQS.delete_message(
            QueueUrl=url,
            ReceiptHandle=message['ReceiptHandle']
        )
    logger.debug('Received and deleted message: %s' % response)
    logger.debug("message: {}".format(message))
    return message

def lambda_handler(event, context):

    """
        Query SQS to get the messages
        Store the relevant info, and pass it to the Elastic Search
    """

    message = getSQSMsg() #data will be a json object
    if message is None:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return

    message_body = json.loads(message["Body"])
    logger.debug("cuisine: {}".format(message_body["cuisine"]))
    cuisine = message_body["cuisine"]

    logger.debug(cuisine)

    location = message_body["location"]
    time = message_body["time"]
    numOfPeople = message_body["people"]
    phoneNumber = message_body["phone"]
    # phoneNumber = "+1" + phoneNumber
    if not cuisine or not phoneNumber:
        logger.debug("No Cuisine or PhoneNum key found in message")
        return

    """
        Query database based on elastic search results
        Store the relevant info, create the message and sns the info
    """

    headers = {'content-type': 'application/json'}
    esUrl = 'https://search-concierge-chatbot-mupjitn6btj57fg6oxgajiffdy.us-east-1.es.amazonaws.com/_search?q={cuisine}'.format(cuisine = cuisine)
    esResponse = requests.get(esUrl, auth=("demo", "Demo@1234"), headers=headers)
    logger.debug("esResponse: {}".format(esResponse.text))
    data = json.loads(esResponse.content.decode('utf-8'))
    logger.info("data: {}".format(data))
    try:
        esData = data["hits"]["hits"]
        logger.info("esData: {}".format(esData))
    except KeyError:
        logger.debug("Error extracting hits from ES response")

    # extract bID from AWS ES
    ids = []
    for restaurant in esData:
        ids.append(restaurant["_source"]["RestaurantID"])

    messageToSend = 'Hello! Here are my {cuisine} restaurant suggestions in {location} for {numPeople} people, at {diningTime}: '.format(
            cuisine=cuisine,
            location=location,
            numPeople=numOfPeople,
            diningTime=time,
        )

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')
    ind = random.sample(ids,10)
    logger.info("ind: {}".format(ind))
    itr = 1
    for id in ind:
        logger.info("id: {}".format(id))
        if itr == 6:
            break
        response = table.scan(FilterExpression=Attr('id').eq(id))
        logger.info("dynamodb response: {}".format(response))
        item = response['Items'][0]
        if response is None:
            continue
        restaurantMsg = '\n' + str(itr) + '. '
        name = item["Name"]
        address = item["Address"]
        restaurantMsg += name +', located at ' + address +'. '
        messageToSend += restaurantMsg
        itr += 1

    messageToSend += "Enjoy your meal!!"
    logger.info("messageToSend: {}".format(messageToSend))

    try:
        client = boto3.client('sns')
        response = client.publish(
            PhoneNumber=phoneNumber,
            Message= messageToSend,
            MessageStructure='string'
        )
    except KeyError:
        logger.debug("Error sending ")
    logger.debug("response - %s",json.dumps(response) )
    logger.debug("Message = '%s' Phone Number = %s" % (messageToSend, phoneNumber))

    return {
        'statusCode': 200,
        'body': json.dumps(messageToSend)
    }
