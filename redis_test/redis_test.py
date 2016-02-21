import logging
import json
import redis
import dateutil.parser

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.DEBUG)

logger.info('Loading function')

# configuration
redis_host = "device.sklzfm.ng.0001.use1.cache.amazonaws.com"
logger.info('redis_host={}'.format(redis_host))
redis_port=6379
logger.info('redis_port={}'.format(redis_port))

# create the client
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, db=0, socket_timeout=1)

def lambda_handler (event, context): 
    logger.debug("Received event: " + json.dumps(event))

    # extract the device key
    device_key = event['device']['device']

    # assume we're not going to overwrite
    next_value = None

    with redis_client.pipeline() as pipe:
        while 1:
            try:
                # put a WATCH on the key that holds our sequence value
                pipe.watch(device_key)
                # after WATCHing, the pipeline is put into immediate execution
                # mode until we tell it to start buffering commands again.
                # this allows us to get the current value of our sequence
                current_value = pipe.get(device_key)
                if current_value is not None:
                        logger.debug('existing entry for key={} found, value={}'.format(device_key, current_value))
			# parse the value
			current_object = json.loads(current_value)
			# see if the new event is newer than the current one 
                        current_timestamp = dateutil.parser.parse(current_object['timestamp'])
                        next_timestamp = dateutil.parser.parse(event['timestamp'])
			if current_timestamp < next_timestamp:
                            logger.debug('replacing existing entry for key={}'.format(device_key))
			    # its newer so overwrite the existing value
			    next_value = json.dumps(event)
                else:
                    logger.debug('no entry found for key={}'.format(device_key))
                    next_value = json.dumps(event)

                # if we have a value to write do it 
                if next_value is not None:
                    logger.debug('writing key={} value={}'.format(device_key, next_value))
                    # now we can put the pipeline back into buffered mode with MULTI
                    pipe.multi()
                    pipe.set(device_key, next_value)                    
 
                # and finally, execute the pipeline
                pipe.execute()

                # if a WatchError wasn't raised during execution, everything
                # we just did happened atomically.
                break

            except redis.WatchError:
                # another client must have changed 'OUR-SEQUENCE-KEY' between
                # the time we started WATCHing it and the pipeline's execution.
                # our best bet is to just retry.
                logger.debug('collision in datastore for key={}, retrying'.format(device_key))
                continue
    
    logger.debug('done, return={}'.format(next_value))
    return next_value
