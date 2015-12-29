import logging
import boto3
import json
import datetime

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)

logger.info('Loading function')

# create a dynamodb client
dynamodb_client = boto3.client('dynamodb')

def lambda_handler (event, context): 
    logger.debug('Received event={}'.format(json.dumps(event)))

    # get today's date
    today = datetime.date.today()
    # compute tomorrow's date
    tomorrow = today + datetime.timedelta(days=1)

    # compute the new table name
    table_name = 'raw_' + str(tomorrow)
    
    try:
        logger.info('checking for  {}...'.format(table_name))        
        table = dynamodb_client.describe_table(TableName=table_name)
        stream_arn = table[u'Table'][u'LatestStreamArn']        
        logger.info('table already exists')
    except:
        logger.info('table not found, creating')
        try:
            # create the table 
            key_schema = [
                {
                    'AttributeName': 'recordId',
                    'KeyType': 'HASH'  #Partition key
                }
            ]
            attribute_definitions =[
                {
                    'AttributeName': 'recordId',
                    'AttributeType': 'S'
                }
            ]
            provisioned_throughput = {
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
            stream_specification = {
                'StreamEnabled': True,
                'StreamViewType': 'NEW_IMAGE'
            }
            #  create the table
            raw_table = dynamodb_client.create_table(TableName=table_name, KeySchema=key_schema, AttributeDefinitions=attribute_definitions, ProvisionedThroughput=provisioned_throughput, StreamSpecification=stream_specification)
        except:
            logger.error('create failed!')
            raise
    finally:
        logger.info('create complete at {}'.format(str(datetime.datetime.now())))
