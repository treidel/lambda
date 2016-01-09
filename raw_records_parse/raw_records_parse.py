import logging
import boto3
import json
import datetime
import decimal
import requests 

# create the logger
logger = logging.getLogger()
# default logging level is INFO
logger.setLevel(logging.INFO)
# lower the logging level for the requests module
logging.getLogger("requests").setLevel(logging.WARNING)

logger.info('Loading function')

# configuration
rest_device_base_url = "https://ojxmlqa12g.execute-api.us-east-1.amazonaws.com/prototype"
logger.info('rest_device_base_url={}'.format(rest_device_base_url))
sns_arn='arn:aws:sns:us-east-1:800774727029:power_records'
logger.info('sns_arn={}'.format(sns_arn))

# create the sns client
sns_client = boto3.client('sns')

def lambda_handler (event, context): 
	logger.debug('Received event={}'.format(json.dumps(event)))

	# cache of devices 
	devices_cache = {}
 
 	# iterate for each record
    	for record in event['Records']:
        	# ignore all but inserts
        	if record['eventName'] != "INSERT": 
            		logger.debug('ignoring event with eventName={}'.format(record['eventName']))
            		continue
		# get the real record
        	new_image = record['dynamodb']['NewImage']
		# get the record elements
        	device = new_image['device']['S']
        	timestamp = new_image['timestamp']['S']
        	uuid = new_image['uuid']['S']
        	duration_in_s = new_image['duration-in-s']['N']
        	
		# see if we have this device cached
		if not device in devices_cache:
			logger.info('querying for device={}'.format(device))
        		# query for the device info
        		response = requests.get(rest_device_base_url + '/devices/' + device)
        		if response.status_code != 200:
            			logger.error('ignoring record with device={} status={}'.format(device, response.status_code))
            			continue
			# parse out the device entity
			device_entity = json.loads(response.text)
			logger.debug(json.dumps(device_entity))
			
			# cache the device
			devices_cache[device] = device_entity
		else:
			# use the cached copy
			device_entity = devices_cache[device]

		# construct the circuit lookup
		circuit_lookup = {}
		for circuit in device_entity['circuits']:
			circuit_name = circuit['name']
			circuit_index = circuit['index']
			logging.debug('mapping name={} to index={}'.format(circuit_name, circuit_index))
			circuit_lookup[circuit_name] = circuit_index
        
        	# iterate through the measurements
        	measurement_entities = {}
        	for circuit_id, measurement in new_image['measurements']['M'].iteritems():
			# map the circuit id to the index
			circuit_index = circuit_lookup[circuit_id]
                	# extract voltage + amperage
                	voltage_in_v = measurement['M']['voltage-in-v']['N']
                	amperage_in_a = measurement['M']['amperage-in-a']['N']
                	# calculate energy consumed in kw-h
                	energy_in_kwh = (float(voltage_in_v) * float(amperage_in_a) * float(duration_in_s)) / float(3600 * 1000)
                	measurement_entity = {'circuit' : circuit_id, 'energy-in-kwh' : energy_in_kwh}
                	measurement_entities[circuit_index] = measurement_entity
        	
		# create the energy consumption entity
        	consumption_entity = {'device' : device_entity, 'timestamp' : timestamp, 'measurements' : measurement_entities}
        
        	# serialize 
        	message = json.dumps(consumption_entity)
        
 		logger.debug('message={}'.format(message))
        
        	# post to the topic
        	logger.debug('posting to SNS')
        	sns_client.publish(TopicArn=sns_arn, Message=message)
        
	logger.debug('finished')
