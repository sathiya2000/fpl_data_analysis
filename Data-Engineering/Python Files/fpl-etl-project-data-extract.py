import json
import requests
import boto3
from datetime import datetime


def lambda_handler(event, context):
    
    base_url = 'https://fantasy.premierleague.com/api/' 
    data = requests.get(base_url+'bootstrap-static/').json() 
    
    #print(data)
    
    client = boto3.client('s3')
    
    fileName = "fpl_raw_data_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = "fpl-etl-project-sathiya",
        Key = "raw_data/to-be-processed/" + fileName,
        Body = json.dumps(data)
        )
    
    print('successfully Done..!')
    