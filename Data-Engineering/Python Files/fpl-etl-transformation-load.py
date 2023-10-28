import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def position(data):
    position_list = []
    for i in data['element_types']:
        position_id = i['id']
        position_name = i['singular_name']
        position_short_name = i['singular_name_short']
        total_player_count = i['element_count']
        position_element = {'position_id':position_id, 'position_name':position_name,'position_short_name':position_short_name,
                           'total_player_count':total_player_count}
        position_list.append(position_element)
    return position_list
        

#Player Data        
def player(data):
    player_list = []
    for i in data['elements']:
        player_id = i['id']
        player_first_name = i['first_name']
        player_last_name = i['second_name']
        player_team_id = i['team']
        player_type = i['element_type']
        minutes_played = i['minutes']
        player_form = i['form']
        player_form_rank = i['form_rank']
        goals_scored = i['goals_scored']
        assists_scored = i['assists']
        clean_sheets = i['clean_sheets']
        goals_conceded = i['goals_conceded']
        own_goals = i['own_goals']
        penalties_saved = i['penalties_saved']
        penalties_missed = i['penalties_missed']
        yellow_cards = i['yellow_cards']
        red_cards = i['red_cards']
        player_influence = i['influence']
        player_influence_rank = i['influence_rank']
        player_creativity = i['creativity']
        player_creativity_rank = i['creativity_rank']
        player_threat = i['threat']
        player_threat_rank = i['threat_rank']
        player_starts = i['starts']
        player_news = i['news']
        news_date = i['news_added']
        player_element = {'player_id':player_id,'first_name':player_first_name,'last_name':player_last_name,'team_id':player_team_id,
                         'player_type':player_type,'minutes_played':minutes_played,'form':player_form,'form_rank':player_form_rank,
                         'goals':goals_scored,'assists':assists_scored,'clean_sheets':clean_sheets,
                         'goals_conceded':goals_conceded,'own_goals':own_goals,'penalties_saved':penalties_saved,
                         'penalties_missed':penalties_missed,'yellow_cards':yellow_cards,'red_cards':red_cards,
                         'player_influence':player_influence,'player_influence_rank':player_influence_rank,
                         'player_creativity':player_creativity,'player_creativity_rank':player_creativity_rank,
                         'player_threat':player_threat, 'player_threat_rank':player_threat_rank,
                         'player_starts':player_starts,'player_news':player_news,'news_date':news_date}
        player_list.append(player_element)
        
    return player_list
        
        

#team data        
def team(data):
    team_list = []
    for i in data['teams']:
        team_id = i['id']
        team_name = i['name']
        team_played = i['played']
        win = i['win']
        loss = i['loss']
        draw = i['draw']
        team_position = i['position']
        team_strength = i['strength']
        team_form = i['form']
        short_name = i['short_name']
        team_code = i['code']
        team_element = {'team_id':team_id,'team_name':team_name,'team_played':team_played,
                        'win':win,'loss':loss,'draw':draw, 'team_position':team_position,'team_strength':team_strength,
                         'team_form':team_form,'short_name':short_name,'team_code':team_code}
        team_list.append(team_element)
        
    return team_list
        
        



def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = 'fpl-etl-project-sathiya'
    Key = 'raw_data/to-be-processed/'
    
    #extract raw data from S3 Bucket
    fpl_data = []
    fpl_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            fpl_data.append(jsonObject)
            fpl_keys.append(file_key)
            
    
    #loop through the extracted data and call the functions    
    for x in fpl_data:
        position_list = position(x)
        player_list = player(x)
        team_list = team(x)
        
        
        
        
        #create dataframe
        position_df = pd.DataFrame.from_dict(position_list)
        
        player_df = pd.DataFrame.from_dict(player_list)
        player_df['news_date'] = pd.to_datetime(player_df['news_date'])
        
        team_df = pd.DataFrame.from_dict(team_list)
        
        
        
        
        #convert dataframes to csv and export to S3 Bucket
        position_key = "transformed_data/player_position_data/position_transformed_data_" + str(datetime.now()) + ".csv"
        position_buffer = StringIO()
        position_df.to_csv(position_buffer)
        position_content = position_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = position_key, Body = position_content)
        
        
        player_key = "transformed_data/player_data/player_transformed_data_" + str(datetime.now()) + ".csv"
        player_buffer = StringIO()
        player_df.to_csv(player_buffer)
        player_content = player_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = player_key, Body = player_content)
        
        
        team_key = "transformed_data/team_data/team_transformed_data" + str(datetime.now()) + ".csv"
        team_buffer = StringIO()
        team_df.to_csv(team_buffer)
        team_content = team_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = team_key, Body = team_content)
        
        
    print('Successfully tranformed JSON Data to CSV and stored in S3')