import math
import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3 as boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


QUEUE_NAME = "DiningBotQueue"
SQS = boto3.client("sqs")


# --- Helpers that build all of the responses ---


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def confirm_intent(session_attributes, intent_name, slots, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ConfirmIntent',
            'intentName': intent_name,
            'slots': slots,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --- Helper Functions ---


def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(n)
    return n

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """

    try:
        return func()
    except KeyError:
        return None


def isvalid_city(city):
    # valid_cities = ['manhattan','new york']
    valid_cities = ['new york']
    return city.lower() in valid_cities


def isvalid_cuisines(cuisine):
    valid_cuisines = ['italian', 'thai', 'american', 'chinese', 'indian','caribbean','korean', 'mexican']
    return cuisine.lower() in valid_cuisines


def build_validation_result(isvalid, violated_slot, message_content):

    if violated_slot == 'DiningTime':
        logger.debug("DiningTime violated: {}".format(message_content))
    else:
        logger.debug("{} - {}".format(violated_slot, message_content))
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def format_and_send_to_sqs(location, cuisine, people, reserve_time, phone):
    # if phone[0] != '+':
    phone = "+1"+phone
    data = {'location': location, 'time': reserve_time,
            'cuisine': cuisine, 'people': people,
            'phone': phone}
    json_data = json.dumps(data)
    send_to_sqs(json_data)

def validate_suggest_dine(slots):
    location = try_ex(lambda: slots['Location'])
    cuisine = try_ex(lambda: slots['Cuisine'])
    dineTime = try_ex(lambda: slots['DiningTime'])
    numPeople = try_ex(lambda: slots['NumPeople'])
    phoneNum = try_ex(lambda: slots['PhoneNum'])

    if location and not isvalid_city(location):
        return build_validation_result(
            False,
            'Location',
            'We currently do not support {} as a valid destination. We are currently only supporting New York as a city.'.format(location)
        )

    if cuisine and not isvalid_cuisines(cuisine):
        return build_validation_result(
            False,
            'Cuisine',
            'We currently only support Italian, Thai, Chinese, Indian, Korean, Caribbean, Mexican and American cuisines. Can you choose from one of these?'
        )

    if dineTime is not None:
        logger.debug("dineTime = {}".format(dineTime))
        if len(dineTime) != 5:
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', 'Invalid time format, please use HH:MM format.')

        hour, minute = dineTime.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'DiningTime', 'Invalid time format, please use HH:MM format.')

        if hour < 10 or hour > 22:
            # Outside of service hours
            return build_validation_result(False, 'DiningTime', 'You can dine in from 10am. to 10pm only.')


    # if numPeople is not None and int(numPeople) > 20 and int(numPeople) < 1:
    if numPeople is not None:
        if int(numPeople) > 20 or int(numPeople) < 1:
            return build_validation_result(
                False,
                'NumPeople',
                'You can host only between 1 to 20 people. Can you provide the valid value in this range?'
            )

    if phoneNum is not None and (len(phoneNum) != 10 or not phoneNum.isdigit()):
        return build_validation_result(
            False,
            'PhoneNum',
            'Please enter a valid 10 digit phone number in the format 1234567890.'
        )

    return {'isValid': True}

""" --- Functions that control the bot's behavior --- """


def dining_suggestions(intent_request):
    """
    Performs dialog management and fulfillment for suggesting a place to dine in.

    Beyond fulfillment, the implementation for this intent demonstrates the following:
    1) Use of elicitSlot in slot validation and re-prompting
    2) Use of sessionAttributes to pass information that can be used to guide conversation
    """
    slots = intent_request['currentIntent']['slots']
    location = slots['Location']
    cuisine = slots['Cuisine']
    dineTime = slots['DiningTime']
    numPeople = slots['NumPeople']
    phoneNum = slots['PhoneNum']
    confirmation_status = intent_request['currentIntent']['confirmationStatus']
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    last_confirmed_reservation = try_ex(lambda: session_attributes['lastConfirmedReservation'])
    if last_confirmed_reservation:
        last_confirmed_reservation = json.loads(last_confirmed_reservation)
    confirmation_context = try_ex(lambda: session_attributes['confirmationContext'])

    # Load confirmation history and track the current reservation.
    reservation = json.dumps({
        'SuggestionType': 'Dining',
        'Location': location,
        'Cuisine': cuisine,
        'DiningTime': dineTime,
        'NumPeople': numPeople,
        'PhoneNum': phoneNum
    })
    session_attributes['currentReservation'] = reservation

    #if location and cuisine and dineTime and numPeople and phoneNum:
        # Generate the price of the car in case it is necessary for future steps.
    #    price = generate_car_price(pickup_city, get_day_difference(pickup_date, return_date), driver_age, car_type)
    #    session_attributes['currentReservationPrice'] = price

    if intent_request['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_suggest_dine(intent_request['currentIntent']['slots'])
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )

        # Determine if the intent (and current slot settings) has been denied.  The messaging will be different
        # if the user is denying a reservation he initiated or an auto-populated suggestion.
        if confirmation_status == 'Denied':
            # Clear out auto-population flag for subsequent turns.
            try_ex(lambda: session_attributes.pop('confirmationContext'))
            try_ex(lambda: session_attributes.pop('currentReservation'))
            if confirmation_context == 'AutoPopulate':
                return elicit_slot(
                    session_attributes,
                    intent_request['currentIntent']['name'],
                    {
                        'Location': None,
                        'Cuisine': None,
                        'DiningTime': None,
                        'NumPeople': None,
                        'PhoneNum': None
                    },
                    'Location',
                    {
                        'contentType': 'PlainText',
                        'content': 'Which city are you going to dine in?'
                    }
                )

            return delegate(session_attributes, intent_request['currentIntent']['slots'])

        if confirmation_status == 'None':
            return delegate(session_attributes, intent_request['currentIntent']['slots'])


        # If confirmation has occurred, continue filling any unfilled slot values or pass to fulfillment.
        if confirmation_status == 'Confirmed':
            # Remove confirmationContext from sessionAttributes so it does not confuse future requests
            try_ex(lambda: session_attributes.pop('confirmationContext'))
            if confirmation_context == 'AutoPopulate':
                if not numPeople:
                    return elicit_slot(
                        session_attributes,
                        intent_request['currentIntent']['name'],
                        intent_request['currentIntent']['slots'],
                        'NumPeople',
                        {
                            'contentType': 'PlainText',
                            'content': 'How many people are you in total?'
                        }
                    )
                elif not cuisine:
                    return elicit_slot(
                        session_attributes,
                        intent_request['currentIntent']['name'],
                        intent_request['currentIntent']['slots'],
                        'Cuisine',
                        {
                            'contentType': 'PlainText',
                            'content': 'What cuisine you want to try?'
                        }
                    )

            return delegate(session_attributes, intent_request['currentIntent']['slots'])

    # Booking the reservation.  In a real application, this would likely involve a call to a backend service.
    logger.debug('suggest dine out at={}'.format(reservation))
    del session_attributes['currentReservation']
    session_attributes['lastConfirmedReservation'] = reservation
    format_and_send_to_sqs(location,cuisine,numPeople,dineTime,phoneNum)
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Thanks, I will search for a place in {} that serves {} food for {} people at {} and send you the details at {}'.format(location,cuisine,numPeople,dineTime,phoneNum)
        }
    )

def greet(intent_request):
    """
    Greets the cutomer when this intent is triggered
    """
    session_attributes = {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hi there, how can I help you?'
        }
    )


def thanks(intent_request):
    """
    Thanks the cutomer when this intent is triggered
    """
    session_attributes = {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Welcome !! Have a nice day ahead.'
        }
    )

# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'Greetings':
        return greet(intent_request)
    elif intent_name == 'DiningSuggestions':
        return dining_suggestions(intent_request)
    elif intent_name == 'ThankYou':
        return thanks(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event: {}'.format(event))
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)



def get_queue_url():
    """Retrieve the URL for the configured queue name"""
    q = SQS.get_queue_url(QueueName=QUEUE_NAME).get('QueueUrl')
    logger.debug("Queue URL is %s", q)
    return q


def send_to_sqs(data):
    """The lambda handler"""
    logger.debug("Sending data to SQS %s", data)
    try:
        url = get_queue_url()
        logger.debug("Got queue URL %s", url)
        resp = SQS.send_message(QueueUrl=url, MessageBody=data)
        logger.debug("Sending data to SQS %s", data)
        logger.debug("Send result: %s", resp)
    except Exception as e:
        raise Exception("Could not record link! %s" % e)
