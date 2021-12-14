###########################
## AWS Lambda that listens to KDS stream and processes the data
###########################
import json
import base64
import boto3

def lambda_handler(event, context):
    dynamodb = boto3.client('dynamodb')
    print('number of records received:', len(event['Records']))
    for record in event['Records']:
       #Kinesis data is base64 encoded so decode here
       payload=base64.b64decode(record["kinesis"]["data"]).decode().split("|")
       
       print("item: " + str(payload))
       precipitation = {
           'stationId': {
               'S': payload[0]
           },
           'stationName': {
               'S': payload[1]
               
           },
           'precipitation': {
               'S': payload[2]
            },
           'snow': {
               'S': payload[3]
            },
           'observedDate': {
               'S': payload[6]
            },
            'insertedTimeStamp': {
               'S': payload[7]
            }
       }
       
       # print("precipitation: " + precipitation)
       # insert data into dynamo DB tables
       dynamodb.put_item(
           TableName='Precipitation', 
           Item = precipitation)
       print("inserted record into Precipitation:", precipitation)
       
       # insert temperature
       temperature = {
           'stationId': {
                   'S': payload[0]
               },
               'stationName': {
                   'S': payload[1]
               },
               'minTemperature': {
                   'S': payload[4]
               },
               'maxTemperature': {
                   'S': payload[5]
               },
               'observedDate': {
                   'S': payload[6]
               },
               'insertedTimeStamp': {
                   'S': payload[7]
               } 
       }
       dynamodb.put_item(
           TableName='Temperature', 
           Item = temperature)
       print("inserted record into Temperature:", temperature)
