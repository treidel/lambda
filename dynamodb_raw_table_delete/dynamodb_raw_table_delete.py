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
    
    logger.debug('Received event={}'.format(json.dumps(event)))

    # get dynamodb object
    dynamodb = boto3.client('dynamodb')

    # calculate the cutoff date
    cutoff = datetime.date.today() - datetime.timedelta(days=1)

    logger.info('purging raw tables older than {}...'.format(str(cutoff)))
    
    try:
        # query a list of tables
        tables = dynamodb.list_tables()
        # iterate throw the list of tables
        for table_name in tables['TableNames']:
            logger.debug('found table={}'.format(table_name))
            if table_name.startswith('raw_'):
                # get the date
                table_date = datetime.datetime.strptime(table_name.split('_')[1], "%Y-%m-%d").date()
                # if the date of the table is before the cutoff drop it 
                if table_date < cutoff:
                    logger.info('table {} old, removing'.format(table_name))
                    dynamodb.delete_table(TableName=table_name)
                else:
                    logger.info('table {} ok, ignoring'.format(table_name))
    except:
        logger.error('purge failed!')
        raise
    finally:
        logger.info('purge complete at {}'.format(str(datetime.datetime.now())))
