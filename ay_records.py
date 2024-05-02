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

team_dict = {'레알루키즈': 'http://alb.or.kr//s/teams/index.php%3Fid%3Dteams_players%26league%3D%EC%97%B0%ED%95%A9%ED%9A%8C%EC%9E%A5%EA%B8%B0%26sc%3D2%26team%3D%EB%A0%88%EC%95%8C%EB%A3%A8%ED%82%A4%EC%A6%88%26order%3Dxb%26sort%3D%26gyear%3D2022',
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
