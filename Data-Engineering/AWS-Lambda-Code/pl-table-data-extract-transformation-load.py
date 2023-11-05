import json
import boto3
from bs4 import BeautifulSoup
import requests
import pandas as pd
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource('s3')
    url = 'https://www.premierleague.com/tables'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html')
    table = soup.find('table')
    col_data = table.find_all('tr')
    Bucket = 'fpl-etl-project-sathiya'
    
    #extract data
    data = []
    for i  in col_data:
        row_data = i.find_all('td')
        indi_row_data = [i.text.split() for i in row_data]
        data.append(indi_row_data)
        
    #retrieve specific data from the data list
    raw_data = []
    for i in data[1:40:2]:
        pos = i[0][0]
        club = i[1][-1]
        played = i[2][0]
        won = i[3][0]
        draw = i[4][0]
        loss = i[5][0]
        GF = i[6][0]
        GA = i[7][0]
        GD = i[8][0]
        points = i[9][0]
        nextm = i[11][-3]
        data_element = {'Position':pos,'Club':club,'Played':played,'Won':won,'Draw':draw,'Loss':loss,
                       'GF':GF,'GA':GA,'GD':GD,'Points':points,'Next Match':nextm}
        raw_data.append(data_element)
    
    
    #print(raw_data)
    #print("Successful Run..!")
    
    
    #create dataframe
    df = pd.DataFrame.from_dict(raw_data)
    
    
    #change datatypes
    convert_dict = {
    'Position' : int,
    'Played' : int,
    'Won' : int,
    'Draw' : int,
    'Loss' : int,
    'GF' : int,
    'GA' : int,
    'GD' : int,
    'Points' : int,
    'Next Match' : str,
    'Club' : str
}

    df = df.astype(convert_dict)
    
    
    #covert to csv & tranfer data to S3
    pl_table_key = "transformed_data/pl_table_data/pl_table_data" + ".csv"
    s3_resource.Object(Bucket, pl_table_key).delete()
    pl_table_buffer = StringIO()
    df.to_csv(pl_table_buffer)
    pl_table_content = pl_table_buffer.getvalue()
    s3.put_object(Bucket = 'fpl-etl-project-sathiya', Key = pl_table_key, Body = pl_table_content)
    
    
    print("Successfully transferred data to S3 Bucket")