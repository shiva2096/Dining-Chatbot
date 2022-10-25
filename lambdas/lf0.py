import json
import boto3

def lambda_handler(event, context):


    client = boto3.client('lex-runtime')
    print("event is: ", event)
    user_id = 'user1'
    bot_name_lex = 'DiningBot'
    bot_alias =  'DiningBot'
    msg_text = event['messages'][0]['unstructured']['text']
    response = client.post_text(
    botName=bot_name_lex ,
    botAlias= bot_alias,
    userId=user_id,
    sessionAttributes={
        'string': 'string'
    },
    requestAttributes={
        'string': 'string'
    },
    inputText= msg_text
)
    bot_response= {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                    "id": 'User1',
                    "text": response['message'],
                    "timestamp": ""
                                     }
                }
                         ]

            }
    return bot_response
