import base64
import boto3
import os
import json
import requests
from requests_aws4auth import AWS4Auth
from datetime import datetime,timedelta
import time

ES_REGION = os.environ['ES_REGION']

region = ES_REGION #
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

ES_INDEX = os.environ['ES_INDEX']
ES_IND_TYPE = os.environ['ES_IND_TYPE']
ES_HOST_HTTP = os.environ['ES_HOST_HTTP']

host = 'https://'+ES_HOST_HTTP
index = ES_INDEX
type = ES_IND_TYPE
url = host + '/' + index + '/' + type + '/'
headers = { "Content-Type": "application/json" }


def handler(event, context):
    print(event)
    count = 0
    for record in event['Records']:
        id='lambda:'+' '+str(count)+' '+str(datetime.now())
        message = base64.b64decode(record['kinesis']['data'])
        msn = json.loads(message)
        print(msn)
        r = requests.put(url +id  , auth=awsauth, json=msn, headers=headers)
        print(r)
        count += 1
    return 'Processed ' + str(count) + ' items.'
