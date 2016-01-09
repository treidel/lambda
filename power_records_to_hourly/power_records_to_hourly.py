import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr
import datetime
import dateutil
from decimal import *

import json

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)

logger.info('Loading function')

# create the dynamodb resource
dynamodb = boto3.resource('dynamodb')

def lambda_handler (event, context): 
    logger.debug("Received event: " + json.dumps(event))
    
    # get the table
    table = dynamodb.Table("hourly")
    
    # iterate through records
    for record in event['Records']:
        # parse the message
        logger.debug('Message: {}'.format(record['Sns']['Message']))
        message = json.loads(record['Sns']['Message'])
        # parse the timestamp
        timestamp = dateutil.parser.parse(message['timestamp'])
        # extract the device entityd
        device_entity = message['device']
        # force everything but the hour part of the time to zero
        date = datetime.datetime(timestamp.year, timestamp.month,timestamp.day, timestamp.hour)
        # create the key for the update request
        key = {'device' : device_entity['device'], 'date' : date.isoformat() + 'Z'}
        # create the attribute updates
        attribute_updates = {}
        for circuit_index, measurement in message['measurements'].iteritems():
            energy_in_kwh = Decimal(str(circuit['energy-in-kwh']))
            attribute_update = {'Action' : 'ADD', 'Value' : energy_in_kwh}
            attribute_updates['energy-in-kwh-{}'.format(circuit_index)] = attribute_update
            
        # create the complete update expression
        logger.debug('key={}, attribute_updates={}'.format(key, attribute_updates))
        # execute the update
        table.update_item(Key=key, AttributeUpdates=attribute_updates)
        
