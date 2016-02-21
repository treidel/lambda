import logging
import boto3
import datetime
import dateutil
from decimal import *
from botocore.exceptions import ClientError
import json

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)

# configuration
check_lambda_function = 'redis_test'

logger.info('Loading function')

# create the SNS client
sns_client = boto3.client('sns')

# create the Lambda client
lambda_client = boto3.client('lambda')

def lambda_handler (event, context): 
    logger.debug("Received event: " + json.dumps(event))
    
    # iterate through records
    for record in event['Records']:
        # parse the message
        logger.debug('Message: {}'.format(record['Sns']['Message']))
        input_message = json.loads(record['Sns']['Message'])

        # call the redis test lambda function (which executes in the VPC)
        logger.debug('calling lambda_function {} to check for out of order records'.format(check_lambda_function))
        response = lambda_client.invoke(FunctionName=check_lambda_function, InvocationType='Event', Payload=json.dumps(input_message))

        # if we did not get a response then there's no need to broadcast this record
        if response is None:
            logger.debug('ignoring out of order record')
            continue
    
        # extract the device entity
        device_entity = input_message['device']
        
        # construct the arn 
        sns_arn='arn:aws:sns:us-east-1:800774727029:device-{}'.format(device_entity['device'])

        logger.info('sns_arn={}'.format(sns_arn))
        
        # create the notification
        notification = {'measurements' : input_message['measurements'], 'timestamp' : input_message['timestamp']}
        
        # serialize
        output_message = json.dumps(notification)

        logger.debug('output_message={}'.format(output_message))

        # post to the topic
        logger.debug('posting to SNS')
        try:  
            sns_client.publish(TopicArn=sns_arn, Message=output_message)
        except ClientError as e:
            if e.response['Error']['Code'] == "NotFound":
                logger.warn('missing topic for device={}'.format(device_entity['device']))
            else:
                raise
