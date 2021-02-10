import boto3
import random
import time
import json

DeliveryStreamName = 'deliveryClickStream'

client = boto3.client('firehose', region_name='us-west-2')

actions = ['clickbtn'+ str(x) for x in range(40)]
names = ["Davi", "Sarah", "Joao P", "Danilo", "Mamai", "Isabela", "Karina"]

record = {}
while True:
    #record['user'] = 'randomic'
    record['action'] = random.choice(actions)
    record['name'] = random.choice(names)
    record['timestamp'] = time.time()
    response = client.put_record(
    DeliveryStreamName=DeliveryStreamName,
	Record={
            'Data': json.dumps(record)
        }
    )
    print('PUTTING RECORD TO KINESIS FIREHOSE: \n' + str(record))
