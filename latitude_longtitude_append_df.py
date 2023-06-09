from pathlib import Path
import pandas as pd
import mojimoji
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
import warnings
from multiprocessing import Pool

warnings.simplefilter('ignore')

def lat_lon_append_df(read_file_path):
    file_name = read_file_path.name
    print(file_name, 'の処理を開始します。')

    save_dir_path = read_file_path.parents[1] / '地図用のデータフレーム'

    if not save_dir_path.exists():
        save_dir_path.mkdir(parents=True)

    save_file_path = Path(save_dir_path, file_name)
    if save_file_path.exists():
        print(file_name, 'は既に処理が済んでいますのでメソッドを終了します。')
        return
        
    df = pd.read_csv(read_file_path)
    df['住所'] = df['住所'].apply(lambda x: mojimoji.zen_to_han(x))
    
    df = df[['県', 'エリア', 'サロン名', 'URL', '住所', 'アクセス', 'カット料金', 'セット面の数', 'ブログ投稿数',
             '口コミ数', 'スタッフ数', '1番人気クーポン名', '1番人気クーポン価格', '2番人気クーポン名', '2番人気クーポン価格',
             '3番人気クーポン名', '3番人気クーポン価格', 'クーポン数', 'メニュー数', 'スタイル数', 'ブログ数',
             '口コミスコアサロン全平均', '雰囲気の全平均', '接客サービスの全平均', '技術・仕上がりの全平均', 'メニュー。料金の全平均',
             'イルミナメニュー化の有無', 'Aujuaメニュー化の有無']]
    
    df = df[~df.duplicated()].reset_index(drop=True)
    
    _list=[]
    for address in df['住所']:
        location_list = get_lat_lon_from_address(address)
        _list.append(location_list)
    _df = pd.DataFrame(_list, columns=['緯度', '経度'])
    
    df = pd.concat(objs=[df, _df], axis=1)

    df.to_csv(save_file_path, index=False)
    print(file_name, 'が正常に出力されましたので処理を終了します。')
    
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

if __name__ == '__main__':
    read_dir_path = Path('/Users/masudaniwabinari/Desktop/hpb_scraping/data/データフレームの前処理')
    file_path_list = list(read_dir_path.glob('*.csv'))
    
    # マルチプロセスはダメ。10秒に一回以上のアクセスとなってしまう。やるならPool(1)    
    # with Pool(1) as p:
    #     list(tqdm(p.imap_unordered(lat_lon_append_df, file_path_list), total=len(file_path_list)))
    for read_file_path in tqdm(file_path_list):
        lat_lon_append_df(read_file_path=read_file_path)