from __future__ import print_function
import boto3
import base64

fh = boto3.client('firehose')

def lambda_handler(event, context):
    rawDataChunks=[event['Records'][x:x+500] for x in range(0, len(event['Records']), 500)]
    for chunk in rawDataChunks:
        count = 0
        batch=[]
        for record in chunk:
            batchDict={}
            payload = base64.b64decode(record['kinesis']['data'])+'\n'
            batchDict['Data']=payload
            batch.append(batchDict)
            count+=1
        try:
            fh.put_record_batch(DeliveryStreamName='Clickstream-Analytics-Streams-FQ2NE464YQXW-RawDatat-7uYYllNqbewm',Records=batch)
            print('Put '+str(count)+' records to Amazon Kinesis Firehose Delivery Stream')
        except Exception as e:
            print(e)