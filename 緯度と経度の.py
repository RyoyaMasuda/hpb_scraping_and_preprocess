import time
import re
import pandas as pd
import numpy as np
import mojimoji
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import lxml

def get_lat_lon_from_address(address: str) -> dict:
    """
    address_lにlistの形で住所を入れてあげると、latlonsという入れ子上のリストで緯度経度のリストを返す関数。
    >>>>get_lat_lon_from_address(['東京都文京区本郷7-3-1','東京都文京区湯島３丁目３０−１'])
    [['35.712056', '139.762775'], ['35.707771', '139.768205']]
    """
    url = 'http://www.geocoding.jp/api/'
    payload = {"v": 1.1, 'q': address}
    r = requests.get(url, params=payload)
    ret = BeautifulSoup(r.content,'lxml')
    if ret.find('error'):
        # raise ValueError(f"Invalid address submitted. {address}")
        time.sleep(10)
        return [0,0]
    else:
        lat = ret.find('lat').string
        lon = ret.find('lng').string
        location_list = [lat, lon]
        time.sleep(10) # sleep(10)は絶対必要
    return location_list
