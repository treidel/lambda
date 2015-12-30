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

# create a dynamodb resource
dynamodb = boto3.resource('dynamodb')

logger.info('Loading function')

def lambda_handler (event, context): 
    logger.debug('Received event={}'.format(json.dumps(event)))

    # iterate through the records
    for record in event['records']:
        # get the record's timestamp
        timestamp = dateutil.parser.parse(record['timestamp'])
        
        # make sure they provided some data
        assert 'circuits' in record
    
        # get the date for the record
        day = timestamp.date()

        # compute the  table name
        table_name = 'raw_' + str(day)
        
        # wrap as a table
        table = dynamodb.Table(table_name)

        # create the circuit entities
        circuit_entities = {}
        for circuit_id, circuit in record['circuits'].iteritems():
            circuit_entity = {
                'voltage-in-v': Decimal(str(circuit['voltage-in-v'])),
                'amperage-in-a' : Decimal(str(circuit['amperage-in-a']))
            }
            circuit_entities[circuit_id] = circuit_entity
            
        # create the record entity
        record_entity = {
            'recordId' : event['device'] + ':' + record['uuid'], 
            'device' : event['device'], 
            'uuid' : record['uuid'],
            'timestamp' : record['timestamp'],
            'duration-in-s' : Decimal(str(record['duration-in-s'])),
            'circuits' : circuit_entities
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
