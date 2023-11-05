import json
import requests
import boto3
from datetime import datetime


def lambda_handler(event, context):
    
    Bucket = 'fpl-etl-project-sathiya'
    Key = 'raw_data/to-be-processed/'
    
    base_url = 'https://fantasy.premierleague.com/api/' 
    data = requests.get(base_url+'bootstrap-static/').json() 
    
    #print(data)
    
    #fixture data
    base_url1 = 'https://fantasy.premierleague.com/api/' 
    data2 = requests.get(base_url1+'fixtures/').json()
    
    
    
    
    client = boto3.client('s3')
    
    #s3_resource = boto3.resource('s3')
    
    #s3_resource.Object(Bucket, Key).delete()
    
    fileName = "fpl_raw_data" + ".json"
    
    fileName1 = "fixture_data" + ".json"
    
    client.put_object(
        Bucket = "fpl-etl-project-sathiya",
        Key = "raw_data/to-be-processed/" + fileName,
        Body = json.dumps(data)
        )
        
    client.put_object(
        Bucket = "fpl-etl-project-sathiya",
        Key = "raw_data/to-be-processed/" + fileName1,
        Body = json.dumps(data2)
        )
    
    print('successfully Done..!')
    