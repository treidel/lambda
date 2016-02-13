import logging
import boto3
import json
import datetime
import dateutil
from decimal import *
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)

logger.info('Loading function')

def lambda_handler (event, context): 
    logger.debug('Received event={}'.format(json.dumps(event)))

    # make sure they provided some data
    assert 'measurements' in event

    # create a dynamodb resource
    dynamodb = boto3.resource('dynamodb')

    # get the record's timestamp
    timestamp = dateutil.parser.parse(event['timestamp'])
        
    # get the date for the record
    day = timestamp.date()

    # compute the  table name
    table_name = 'raw_' + str(day)
        
    # wrap as a table
    table = dynamodb.Table(table_name)

    # create the measurement entities
    measurement_entities = {}
    for circuit_id, measurement in event['measurements'].iteritems():
        measurement_entity = {
            'voltage-in-v': Decimal(str(measurement['voltage-in-v'])),
            'amperage-in-a' : Decimal(str(measurement['amperage-in-a']))
        }
        measurement_entities[circuit_id] = measurement_entity
            
    # create the record entity
    record_entity = {
        'recordId' : event['device'] + ':' + event['uuid'], 
        'device' : event['device'], 
        'uuid' : event['uuid'],
        'timestamp' : event['timestamp'],
        'duration-in-s' : Decimal(str(event['duration-in-s'])),
        'measurements' : measurement_entities
    }
        
    # setup the conditional expression that the record not already exist
    condition = Attr("recordId").not_exists()
    
    try:
        logger.debug('writing record={}'.format(record_entity))
        table.put_item(Item=record_entity, ConditionExpression=condition)
    except ClientError as e:
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            logger.info('duplicate record for device=' + record_entity['device'] + ' uuid=' + record_entity['uuid'])
        elif e.response['Error']['Code'] == "ResourceNotFoundException":
            logger.info('ignoring stale record for device=' + record_entity['device'] + ' uuid=' + record_entity['uuid'] + ' timestamp=' + record_entity['timestamp'])
        else:
            raise
        
    logger.debug('write complete at {}'.format(str(datetime.datetime.now())))
