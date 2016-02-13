import logging
import boto3
import json
import datetime

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)

logger.info('Loading function')

def lambda_handler (event, context): 
    
    logger.debug("Received event: " + json.dumps(event))

    # get dynamodb object
    dynamodb_client = boto3.client('dynamodb')

    # get lambda object
    lambda_client = boto3.client('lambda')
    
    # list of arns we care about 
    stream_arns = []
    
    try:
         # query a list of tables
        tables = dynamodb_client.list_tables()
        # iterate throw the list of tables
        for table_name in tables['TableNames']:
            logger.debug('found table:{}'.format(table_name))
            if table_name.startswith('raw_'):
                # describe the table
                table = dynamodb_client.describe_table(TableName=table_name)
                # see if we have an arn
                if 'LatestStreamArn' in table[u'Table']:
                    # get the arn 
                    stream_arn = table[u'Table'][u'LatestStreamArn']   
                    logger.info('found stream_arn:{} for table_name:{}'.format(stream_arn, table_name))
                    # store in list
                    stream_arns.append(stream_arn)
        
        logger.info('stream_arns: ' + str(stream_arns))     
        
        # query the list of event sources
        mappings = lambda_client.list_event_source_mappings(FunctionName='raw_records_parse')
        for mapping in mappings['EventSourceMappings']:
            event_arn = mapping['EventSourceArn']
            if event_arn in stream_arns:
                logger.info('event_arn:{} already an event source'.format(event_arn))
                stream_arns.remove(event_arn)
            else:
                uuid = mapping['UUID']
                logger.info('removing event_arn:{} with UUID:{}'.format(event_arn, uuid))
                lambda_client.delete_event_source_mapping(UUID=uuid)
        
        # now add any event sources not currently tracked
        for stream_arn in stream_arns:
            logger.info('adding stream_arn:{}'.format(stream_arn))
            lambda_client.create_event_source_mapping(EventSourceArn=stream_arn, FunctionName='raw_records_parse', Enabled=True, BatchSize=100, StartingPosition='LATEST')
        
    except:
        logger.error('poll failed!')
        raise
    finally:
        logger.info('poll complete at {}'.format(str(datetime.datetime.now())))
