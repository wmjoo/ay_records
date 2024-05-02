import requests
from bs4 import BeautifulSoup
import bs4
import re
import pandas as pd
import numpy as np
import requests
from urllib import parse
import datetime
import requests
import os
import pickle
from multiprocessing import Pool, cpu_count, Value, Lock
import streamlit as st

team_names_list = ['레알루키즈', '안양실버', '안양도깨비', '맥퀸즈클럽As', '천군', '과천토리즈', '해머스1999', '경기블루오션', '보이스캐디구디스', '안양동안경찰서', 'Slugger', '야르셀로나배트홀릭스']

team_dict = {'레알루키즈': 'http://alb.or.kr/s/teams/index.php?id=teams_players&league=%BF%AC%C7%D5%C8%B8%C0%E5%B1%E2&sc=2&team=%B7%B9%BE%CB%B7%E7%C5%B0%C1%EE&order=xb&sort=',
 '안양실버': 'http://alb.or.kr/s/teams/index.php?id=teams_players&league=%BF%AC%C7%D5%C8%B8%C0%E5%B1%E2&sc=2&team=%BE%C8%BE%E7%BD%C7%B9%F6&order=xb&sort=&gyear=2024',
 '안양도깨비': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EC%95%88%EC%96%91%EB%8F%84%EA%B9%A8%EB%B9%84%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '맥퀸즈클럽As': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EB%A7%A5%ED%80%B8%EC%A6%88%ED%81%B4%EB%9F%BDAs%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '천군': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EC%B2%9C%EA%B5%B0%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '과천토리즈': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EA%B3%BC%EC%B2%9C%ED%86%A0%EB%A6%AC%EC%A6%88%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '해머스1999': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%ED%95%B4%EB%A8%B8%EC%8A%A41999%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '경기블루오션': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EA%B2%BD%EA%B8%B0%EB%B8%94%EB%A3%A8%EC%98%A4%EC%85%98%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '보이스캐디구디스': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EB%B3%B4%EC%9D%B4%EC%8A%A4%EC%BA%90%EB%94%94%EA%B5%AC%EB%94%94%EC%8A%A4%26order%3Dxb%26sort%3D%26gyear%3D2022',
 '안양동안경찰서': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EC%95%88%EC%96%91%EB%8F%99%EC%95%88%EA%B2%BD%EC%B0%B0%EC%84%9C%26order%3Dxb%26sort%3D%26gyear%3D2022',
 'Slugger': 'http://alb.or.kr/s/teams/index.php?id=teams_players&league=%BF%AC%C7%D5%C8%B8%C0%E5%B1%E2&sc=2&team=Slugger&order=xb&sort=&gyear=',
 '야르셀로나배트홀릭스': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EC%95%BC%EB%A5%B4%EC%85%80%EB%A1%9C%EB%82%98%EB%B0%B0%ED%8A%B8%ED%99%80%EB%A6%AD%EC%8A%A4%26order%3Dxb%26sort%3D%26gyear%3D2022'}

# 세션 초기화
session = requests.Session()
session.headers.update({'Accept-Encoding': 'gzip, deflate'})

# 각 팀의 데이터를 가져오는 함수
def fetch_team_data(team_name):
    t0 = datetime.datetime.now()
    # print(t0.strftime('%y%m%d_%H%M%S'))
    
    url = team_dict[team_name]
    # print('\t', team_name, url)
    
    response = requests.get(url)
    soup_team = BeautifulSoup(response.text, 'lxml') # 'html.parser')
    
    t1 = datetime.datetime.now()
    # print(t1-t0, t1.strftime('%y%m%d_%H%M%S'), team_name, url)
    
    return team_name, soup_team

# ThreadPoolExecutor 사용
team_data = {}
with ThreadPoolExecutor(max_workers=100) as executor:
    future_to_team = {executor.submit(fetch_team_data, team): team for team in team_dict.keys()}
    for future in future_to_team:
        team_name, data = future.result()
        team_data[team_name] = data
        print(f"Data fetched for {team_name}")

# 실행 완료 확인
print("All data fetched")

######################################################
#
######################################################

team_players_dict = dict()
for team_name in team_names_list:
    # HTML 소스를 BeautifulSoup 객체로 생성
    soup = team_data[team_name]
    # '레알루키즈' 팀의 선수 등록 현황 테이블을 찾기
    team_table = soup.find_all('table')[12]

    # 선수별로 행을 반복하여 정보 추출
    player_data = []
    for row in team_table.find_all('tr')[1:]:  # 테이블의 첫 행은 헤더이므로 건너뜁니다.
        cols = row.find_all('td')
        if len(cols) > 2:  # 선수 정보가 담긴 행 확인
            player_number = cols[3].text.strip()
            player_pos = cols[4].text.strip()
            player_school = cols[8].text.strip()
            player_name_link = cols[2].find('a')
            if player_name_link:
                player_name = player_name_link.text.strip()
                player_url = player_name_link['href']
            else:
                player_name = cols[2].text.strip()
                player_url = "No URL"
            
            # 선수 정보 저장
            player_data.append({
                'Number': player_number,
                'POS': player_pos,
                'Name': player_name,
                'School': player_school,
                'URL': player_url
            })

    # 판다스 데이터프레임으로 변환
    tmp_df = pd.DataFrame(player_data)
    print(team_name, tmp_df.shape)
    team_players_dict.setdefault(team_name, tmp_df)

print(team_players_dict.keys())

######################################################
#
######################################################

team_player_dict = dict()
# team_player_dict = dict()
# team_player_dict.setdefault(team_name, player_dict)

# team_player_dict

for team_name in teams_vs2024:
    # print(team_name)
    team_players_df = team_players_dict[team_name][['Name', 'URL']].sort_values('Name')

    # 첫 번째 열을 키로, 두 번째 열을 값으로 하는 딕셔너리 생성
    result_dict = pd.Series(team_players_df['URL'].values, index=team_players_df['Name']).to_dict()

    team_player_dict.setdefault(team_name, result_dict)
print(team_player_dict)


## 230902 업데이트 버전
def player_record_crawling(#player_dict, player_name, 
                           player_url, record_cat, player_record_idx = {'통산타격':12, '시즌타격':14, '경기별타격' : 16,
                     '통산투구':18, '시즌투구':20, '경기별투구' : 22}):
    df2_dict = dict()
    print(player_url)
    response = requests.get(player_url)
    soup_player = BeautifulSoup(response.text, 'html.parser')
    table_all2 = soup_player.find_all('table')
    globals()['table_all2'] = table_all2

    # 선수 프로필 추출
    player_info = table_all2[10]
    inner_table = player_info.find('table', border="0", cellpadding="0", cellspacing="0", width="100%")
    rows = inner_table.find_all('tr')
    data = {}
    for row in rows:
        columns = row.find_all('td')
        key = columns[0].text.strip()
        value = columns[2].text.strip()
        data[key] = value

    # 데이터를 DataFrame으로 변환
    player_info_df = pd.DataFrame([data])
    player_name = player_info_df['선수명']
    player_no   = player_info_df['배번'] # table2.find('td', {'class': 'title_hanw'}).text.strip().split(' ')[1]        
    # display(player_info_df)    
    
    for rcd_cat in record_cat:
        print(rcd_cat)
        table2 = table_all2[player_record_idx[rcd_cat]]
       
        # Extract table header
        header = []
        for th in table2.find_all('td', {'class': 'title_eng'}):
            header.append(th.text.strip())

        # Extract table rows
        rows = []
        for tr in table2.find_all('tr'): # .find_all('td')
            row = []
            for td in tr.find_all('td'):
                row.append(td.text.strip())
                rows.append(row)

        # Create a pandas DataFrame from the list of rows and header
        df2 = pd.DataFrame(rows[2:])
        df2.columns = df2.iloc[0]

        # 첫 번째 행 삭제
        df2.insert(0, 'Name', player_name)
        df2.insert(0, 'No',   player_no)
        df2.insert(1, 'record_cat', rcd_cat)
        df2 = df2[1:].reset_index(drop=True)
        df2 = df2.mask(df2 == '').fillna(0)
        df2 = df2.drop_duplicates().reset_index(drop=True)


        if rcd_cat[:3] == '경기별':
            anchor_tags = table2.find_all("a")
            filtered_anchor_tags = [anchor_tag for anchor_tag in anchor_tags if anchor_tag["href"].startswith("/z")] #
            game_dict = dict()
            game_url_list = list()
            for anchor_tag in filtered_anchor_tags:
                game_name = anchor_tag.text.strip()
                game_url = 'http://alb.or.kr/' + anchor_tag["href"]
                game_dict.setdefault(game_name, game_url)
                game_url_list.append(game_url)

            # print( game_dict )
            df2['Game_url'] = game_name #game_url_list

            if rcd_cat == '경기별타격':            
                df2.columns = list(df2.columns[:2]) + list(df2.columns[3:]) + ['last_col']
                df2 = df2.iloc[2:]
            if rcd_cat == '경기별투구':
                df2.columns = ['Name', 'record_cat', 'TITLE', '소속연도', 'DATE', 'VS', 'NO', 'RESULT',
                            'SHIFT', 'IP', 'TBF', 'NP', 'HIT', 'HR', 'BB', 'HBP', 'SO', 'RA', 'ER', 'ERA', 'Game_url']
        df2_dict.setdefault(rcd_cat, df2)
    return df2_dict

'''
def player_record_crawling(player_dict, player_name, record_cat, player_record_idx = {'통산타격':12, '시즌타격':14, '경기별타격' : 16,
                     '통산투구':18, '시즌투구':20, '경기별투구' : 22}):
    # print(player_name)
    url = player_dict[player_name] #'http://alb.or.kr/z/view.php?id=players&category=2&no=13339'
    response = requests.get(url)
    soup_player = BeautifulSoup(response.text, 'html.parser')
    table_all2 = soup_player.find_all('table')
    table2 = table_all2[player_record_idx[record_cat]]
    # Extract table header
    header = []
    for th in table2.find_all('td', {'class': 'title_eng'}):
        header.append(th.text.strip())

    # Extract table rows
    rows = []
    for tr in table2.find_all('tr'): # .find_all('td')
        row = []
        for td in tr.find_all('td'):
            row.append(td.text.strip())
        #print(row)
        if row != ['']:
            rows.append(row)

    # Create a pandas DataFrame from the list of rows and header
    df2 = pd.DataFrame(rows[2:])
    df2.columns = df2.iloc[0]

    # 첫 번째 행 삭제
    df2.insert(0, 'Name', player_name)
    df2.insert(1, 'record_cat', record_cat)
    df2 = df2[1:].reset_index(drop=True)
    df2 = df2.mask(df2 == '').fillna(0)

    if record_cat[:3] == '경기별':
        anchor_tags = table2.find_all("a")
        filtered_anchor_tags = [anchor_tag for anchor_tag in anchor_tags if anchor_tag["href"].startswith("/z")] #
        game_dict = dict()
        game_url_list = list()
        for anchor_tag in filtered_anchor_tags:
            game_name = anchor_tag.text.strip()
            game_url = 'http://alb.or.kr/' + anchor_tag["href"]
            game_dict.setdefault(game_name, game_url)
            game_url_list.append(game_url)

        # print( game_dict )
        df2['Game_url'] = game_url_list

        if record_cat == '경기별투구':
            df2.columns = ['Name', 'record_cat', 'TITLE', '소속연도', 'DATE', 'VS', 'NO', 'RESULT',
                        'SHIFT', 'IP', 'TBF', 'NP', 'HIT', 'HR', 'BB', 'HBP', 'SO', 'RA', 'ER', 'ERA', 'Game_url']
    return df2

'''

tot_player_dict = team_player_dict.copy()


import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

def fetch_batter_data(team_name, player_name):
    batter_df = player_record_crawling(player_dict=tot_player_dict[team_name], player_name=player_name, record_cat='통산타격')
    batter_df = batter_df[bat_sorted_cols]
    batter_df[bat_int_cols] = batter_df[bat_int_cols].astype('float').astype('int')
    batter_df[bat_float_cols] = batter_df[bat_float_cols].astype('float')
    batter_df['OPS'] = batter_df[['OBP', 'SLG']].sum(axis=1)
    batter_df['team_name'] = team_name
    return batter_df

def main():
    all_batter_data = pd.DataFrame()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for team_name, players in tot_player_dict.items():
            for player_name in players:
                future = executor.submit(fetch_batter_data, team_name, player_name)
                futures.append(future)
        
        for future in as_completed(futures):
            batter_data = future.result()
            all_batter_data = pd.concat([all_batter_data, batter_data], ignore_index=True)
            import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

def fetch_team_batter_data(team_name):
    player_dict = tot_player_dict[team_name]
    player_name_lst = sorted(list(player_dict.keys()))
    team_batter_df = pd.DataFrame()

    for nm in player_name_lst:
        batter_df = player_record_crawling(player_dict=player_dict, player_name=nm, record_cat='통산타격')
        if team_batter_df.empty:
            team_batter_df = batter_df
        else:
            team_batter_df = pd.concat([team_batter_df, batter_df], ignore_index=True)

    team_batter_df = team_batter_df[bat_sorted_cols]
    team_batter_df[bat_int_cols] = team_batter_df[bat_int_cols].astype('float').astype('int')
    team_batter_df[bat_float_cols] = team_batter_df[bat_float_cols].astype('float')
    team_batter_df['OPS'] = team_batter_df[['OBP', 'SLG']].sum(axis=1)
    team_batter_df['team_name'] = team_name

    return team_batter_df

def main():
    allteams_tot_batter_df = pd.DataFrame()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_team = {executor.submit(fetch_team_batter_data, team): team for team in list(tot_player_dict.keys())[:2]}
        for future in as_completed(future_to_team):
            team_batter_df = future.result()
            allteams_tot_batter_df = pd.concat([allteams_tot_batter_df, team_batter_df], ignore_index=True)
            print(f"Data fetched for {future_to_team[future]} - Complete")

    allteams_tot_batter_df.to_csv('토요리그_{}_타자_통산_{}.csv'.format(allteams_tot_batter_df.shape[0], datetime.datetime.now().strftime('%y%m%d_%H%M%S')), index=False, encoding='cp949')

if __name__ == "__main__":
    main()
    # all_batter_data.to_csv('토요리그_{}_타자_통산_{}.csv'.format(all_batter_data.shape[0], datetime.datetime.now().strftime('%y%m%d_%H%M%S')), index=False, encoding='cp949')
    print("All batter data fetched and saved successfully.")
