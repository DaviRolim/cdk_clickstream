import json
import boto3
import random
import datetime
import os
import time

kinesis = boto3.client('kinesis')
RefStreamName = os.getenv('STREAM_NAME')

def getReferrer():
    x = random.randint(1,2)
    x = x*10
    y = x+50
    data = {}
    product = random.randint(1,250)
    data['custid'] = random.randint(x,y)
    data['trafficfrom'] = random.choice(['amazon.com', 'facebook.com', 'twitter.com','amazon.com','google','email_CampanhaBeer', 'email_MyFirstBeer','Alexa','BeerHug_TV','Alexa', 'Jao_Cervejeiro'])
    data['url'] = random.choice(['beer_vitrine_nav','beer_product_detail','beer_vitrine_nav','beer_checkout','beer_product_detail','beer_cart'])
    data['device'] = random.choice(['app_mobile','app_tablet','browser'])
    data['touchproduct'] = 0
    now = datetime.datetime.now()
    time.sleep(.5)
    str_now = now.isoformat()
    data['trans_timestamp'] = str_now
    if data['url'] != 'beer_vitrine_nav':
          data['touchproduct'] = product
    return data

def lambda_handler(event, context):
   begining = datetime.datetime.now()
   newtime = begining
   while (newtime - begining).total_seconds()<55:
        data = json.dumps(getReferrer())
        #print(data)
        kinesis.put_record(
                StreamName=RefStreamName,
                Data=data,
                PartitionKey='partitionkey')
        newtime = datetime.datetime.now()
   print ('Cleaned end execution.')
