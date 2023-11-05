import json
import boto3
import requests
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
    
    
    
#fixture Data
def fixture(data):
    fixture_list = []
    for i in data:
        match_id = i['id']
        gameWeek = i['event']
        finished = i['finished']
        started = i['started']
        kickoff_time = i['kickoff_time']
        minutes = i['minutes']
        away_team = i['team_a']
        away_score = i['team_a_score']
        home_team = i['team_h']
        home_score = i['team_h_score']
        team_code = i['code']
        home_diff = i['team_h_difficulty']
        away_diff = i['team_a_difficulty']
        pulse_id = i['pulse_id']
        fixture_element = {'match_id':match_id,'gameWeek':gameWeek,'kickoff_time':kickoff_time,
                        'started':started,'finished':finished,'minutes':minutes, 'home_team':home_team,'home_score':home_score,
                         'away_team':away_team,'away_score':away_score,'home_diff':home_diff,'away_diff':away_diff,
                          'code':team_code, 'pulse_id':pulse_id}
        fixture_list.append(fixture_element)
        
    return fixture_list 



def lambda_handler(event, context):
    s3 = boto3.client("s3")
    Bucket = 'fpl-etl-project-sathiya'
    Key = 'raw_data/to-be-processed/'
    s3_resource = boto3.resource('s3')
    
    #extract raw data from S3 Bucket
    fpl_data = []
    fpl_keys = []
    fix_data = []
    fix_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix = Key)['Contents']:
        #print(file)
        file_key = file['Key']
        #print(file_key)
        if file_key.split('/')[2] == 'fpl_raw_data.json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            fpl_data.append(jsonObject)
            fpl_keys.append(file_key)
        elif file_key.split('/')[2] == 'fixture_data.json':
            response1 = s3.get_object(Bucket = Bucket, Key = file_key)
            content1 = response1['Body']
            jsonObject1 = json.loads(content1.read())
            fix_data.append(jsonObject1)
            fix_keys.append(file_key)
            
    #print(fpl_keys)
    #print(fix_keys)
    #print(fix_data)
    #print(fpl_data)
    
    for n in fix_data:
        fixture_list = fixture(n)
        fixture_df = pd.DataFrame.from_dict(fixture_list)
        fixture_df = fixture_df.drop([0])
        fixture_df.drop(index=fixture_df.index[0], axis=0, inplace=True)
        fixture_df = fixture_df.tail(-1)
        fixture_df=fixture_df.fillna('0')
        fixture_df['kickoff_time'] = pd.to_datetime(fixture_df['kickoff_time'])
        #convert datatypes of columns
        convert_dict = {
            'gameWeek':int,
            'home_score':int,
            'away_score':int
        }
        fixture_df = fixture_df.astype(convert_dict)
        
        #convert to CSV
        fixture_key = "transformed_data/fixture_data/fixture_transformed_data" + ".csv"
        #delete the old file
        s3_resource.Object(Bucket, fixture_key).delete() 
        
        fixture_buffer = StringIO()
        fixture_df.to_csv(fixture_buffer)
        fixture_content = fixture_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = fixture_key, Body = fixture_content)
        
        print("fixture data success")
    
    
    #individual player Data per GameWeek
    player_list1 = []
    for x in range(1,38,1):
        base_url = 'https://fantasy.premierleague.com/api/event/'+ str(x) +'/live/' 
        data3 = requests.get(base_url).json() 
        for i in data3['elements']:
            player_id = i['id']
            minutes_played = i['stats']['minutes']
            goals = i['stats']['goals_scored']
            assists = i['stats']['assists']
            clean_sheets = i['stats']['clean_sheets']
            goals_conceded = i['stats']['goals_conceded']
            own_goals = i['stats']['own_goals']
            penalties_saved = i['stats']['penalties_saved']
            penalties_missed = i['stats']['penalties_missed']
            yellow_cards = i['stats']['yellow_cards']
            red_cards = i['stats']['red_cards']
            saves = i['stats']['saves']
            bps = i['stats']['bps']
            player_influence = i['stats']['influence']
            player_creativity = i['stats']['creativity']
            player_threat = i['stats']['threat']
            player_starts = i['stats']['starts']
            ict_index = i['stats']['ict_index']
            expected_goals = i['stats']['expected_goals']
            expected_assists = i['stats']['expected_assists']
            expected_goal_involvements = i['stats']['expected_goal_involvements']
            expected_goals_conceded = i['stats']['expected_goals_conceded']
            total_points = i['stats']['total_points']
            in_dreamteam = i['stats']['in_dreamteam']
            if i['explain'] != []:
                fixture_id = i['explain'][0]['fixture']
            player_element1 = {'player_id':player_id,'minutes_played':minutes_played,'goals':goals,
                            'assists':assists,'clean_sheets':clean_sheets,'goals_conceded':goals_conceded, 'own_goals':own_goals,'penalties_saved':penalties_saved,
                             'penalties_missed':penalties_missed,'yellow_cards':yellow_cards,'red_cards':red_cards,
                              'saves':saves, 'bps':bps,'player_influence':player_influence,'player_creativity':player_creativity,'player_threat':player_threat,
                             'player_starts':player_starts,'ict_index':ict_index,'expected_goals':expected_goals,'expected_assists':expected_assists,'expected_goal_involvements':expected_goal_involvements,
                             'expected_goals_conceded':expected_goals_conceded,'total_points':total_points,'in_dreamteam':in_dreamteam,'fixture_id':fixture_id,'gameWeek':x}
            player_list1.append(player_element1)
    individual_player_df = pd.DataFrame.from_dict(player_list1)
    
    #convert to CSV
    individual_player_key = "transformed_data/individual_player_data/individual_player_data" + ".csv"
    
    #delete the old file
    s3_resource.Object(Bucket, individual_player_key).delete()
    
    individual_buffer = StringIO()
    individual_player_df.to_csv(individual_buffer)
    individual_content = individual_buffer.getvalue()
    s3.put_object(Bucket = Bucket, Key = individual_player_key, Body = individual_content)

    
    
    
    
    
    
    
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
        position_key = "transformed_data/player_position_data/position_transformed_data"  + ".csv"
        s3_resource.Object(Bucket, position_key).delete()
        position_buffer = StringIO()
        position_df.to_csv(position_buffer)
        position_content = position_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = position_key, Body = position_content)
        
        
        player_key = "transformed_data/player_data/player_transformed_data" + ".csv"
        s3_resource.Object(Bucket, player_key).delete()
        player_buffer = StringIO()
        player_df.to_csv(player_buffer)
        player_content = player_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = player_key, Body = player_content)
        
        
        team_key = "transformed_data/team_data/team_transformed_data" + ".csv"
        s3_resource.Object(Bucket, team_key).delete()
        team_buffer = StringIO()
        team_df.to_csv(team_buffer)
        team_content = team_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = team_key, Body = team_content)
        
    
    new_keys = fpl_keys + fix_keys    
    for i in new_keys:
        copy_resource = {
            'Bucket': Bucket,
            'Key' : i
        }
        s3_resource.meta.client.copy(copy_resource, Bucket, 'raw_data/processed/' + i.split('/')[-1])
        s3_resource.Object(Bucket, i).delete()
        
    print('Successfully tranformed JSON Data to CSV and stored in S3')