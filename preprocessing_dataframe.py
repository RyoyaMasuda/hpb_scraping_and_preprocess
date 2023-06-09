import time
import re
import sys
import pandas as pd
import numpy as np
import mojimoji
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import lxml
import re
import regex
import warnings
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool
import logging

logger = logging.getLogger(__name__)

# ログを保存するディレクトリが無ければ作成する。
log_save_dir = "./log/"
if not Path(log_save_dir).exists():
    Path(log_save_dir).mkdir(parents=True)

# ログ周りの設定
logger.setLevel(10)

formatter = logging.Formatter('時間:%(asctime)s 行:%(lineno)d ログレベル:%(levelname)s メッセージ:%(message)s')

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# ログファイルの追加設定(ファイル出力)
start_time = datetime.datetime.today().strftime('%Y-%m-%d_%H:%M:%Sstart')
fh = logging.FileHandler(f'./log/preprocess_df_{start_time}.log')
logger.addHandler(fh)
fh.setFormatter(formatter)

warnings.simplefilter('ignore')
# pd.set_option('display.max_columns', None)

def preprocess_dataframe(path: Path) -> None:
    file_name = path.name
    
    save_dir_path = path.parents[1] / 'データフレームの前処理'

    if not save_dir_path.exists():
        save_dir_path.mkdir(parents=True)
    save_file_path = Path(save_dir_path, file_name)
    
    if save_file_path.exists():
        logger.info(f'{file_name}は既に存在するので処理は行いません。')
        return
        
    logger.info(f'{file_name}の処理を開始します。')
    try:
        df = pd.read_csv(path)
        df['エリア'] = df['エリア'].str.replace('の美容院・美容室・ヘアサロン', '')

        df['投稿日時'] = df['投稿日時'].str.replace('[投稿日] ', '', regex=False)
        df['セット面の数'] = df['セット面の数'].str.replace('セット面', '').str.replace('席', '')
        df['カット料金'] = df['カット料金'].str.replace('¥', '').str.replace(',', '').str.replace('～', '').str.replace('~', '')
        df['ブログ投稿数'] = df['ブログ投稿数'].str.replace('件', '').str.replace('　', '').str.replace(' ', '').str.replace('UP', '')
        df['ブログ投稿数'] = df['ブログ投稿数'].fillna(0)
        df['口コミ数'] = df['口コミ数'].str.replace('件', '').str.replace('　', '').str.replace(' ', '').str.replace('UP', '')
        df['口コミ数'] = df['口コミ数'].fillna(0)
        df.rename(columns={'口コミ数1番人気クーポン名':'1番人気クーポン名'}, inplace=True)

        df[['1番人気クーポン名','2番人気クーポン名','3番人気クーポン名']] = df[['1番人気クーポン名','2番人気クーポン名','3番人気クーポン名']].fillna('')

        df['1番人気クーポン価格'] = df['1番人気クーポン価格'].str.replace('¥','').str.replace(',', '')
        df['2番人気クーポン価格'] = df['2番人気クーポン価格'].str.replace('¥','').str.replace(',', '')
        df['3番人気クーポン価格'] = df['3番人気クーポン価格'].str.replace('¥','').str.replace(',', '')

        df[['1番人気クーポン価格','2番人気クーポン価格','3番人気クーポン価格']] = df[['1番人気クーポン価格', '2番人気クーポン価格', '3番人気クーポン価格']].fillna(0)
        df['初回予約金額'].fillna(0, inplace=True)

        df = df.reset_index(drop=True)

        _list_for_df = []
        for _list in df['初回予約金額'].str.replace('¥', '').str.replace(',', '').str.split('～'):
            if  _list is np.nan or len(_list) < 2:
                _list=['0', '0']
            _list_for_df.append(_list)
            
        df = pd.concat([df, pd.DataFrame(_list_for_df, columns=['初回予約金額最小', '初回予約金額最大'])], axis=1)

        df.drop(['初回予約金額'], axis=1, inplace=True)

        df['2回目以降予約金額'].fillna(0, inplace=True)

        _list_for_df = []
        for _list in df['2回目以降予約金額'].str.replace('¥', '').str.replace(',', '').str.split('～'):
            if  _list is np.nan or len(_list) < 2:
                _list=['0', '0']
            _list_for_df.append(_list)
            
        df = df.reset_index(drop=True)

        df = pd.concat([df, pd.DataFrame(_list_for_df, columns=['2回目以降予約金額最小', '2回目以降予約金額最大'])], axis=1)
        df.drop(['2回目以降予約金額'], axis=1, inplace=True)
        df['クーポン数'] = df['クーポン数'].fillna(0).astype(int)
        df['口コミ数'] = df['口コミ数'].astype(int)

        df['カット料金'] = df['カット料金'].str.replace('-', '0')
        df['カット料金'] = df['カット料金'].astype(int)

        df['セット面の数'] = df['セット面の数'].str.replace('-', '0')
        df['セット面の数'] = df['セット面の数'].astype(int)

        df['1番人気クーポン価格'] = df['1番人気クーポン価格'].astype(str).str.replace('‐', '0')
        df['2番人気クーポン価格'] = df['2番人気クーポン価格'].astype(str).str.replace('‐', '0')
        df['3番人気クーポン価格'] = df['3番人気クーポン価格'].astype(str).str.replace('‐', '0')
        df['ブログ投稿数'] = df['ブログ投稿数'].astype(int)

        df[['スタイル数', 'ブログ数',
            '口コミスコアサロン全平均', '雰囲気の全平均', '接客サービスの全平均', '技術・仕上がりの全平均', 'メニュー。料金の全平均',
            '総合', '雰囲気', '接客サービス', '技術・仕上がり', 'メニュー・料金',]]=\
        df[['スタイル数', 'ブログ数',
            '口コミスコアサロン全平均', '雰囲気の全平均', '接客サービスの全平均', '技術・仕上がりの全平均', 'メニュー。料金の全平均',
            '総合', '雰囲気', '接客サービス', '技術・仕上がり', 'メニュー・料金',]].fillna(0)

        df[['スタイル数', 'ブログ数', '総合', '雰囲気', '接客サービス', '技術・仕上がり', 'メニュー・料金',]]=\
        df[['スタイル数', 'ブログ数', '総合', '雰囲気', '接客サービス', '技術・仕上がり', 'メニュー・料金',]].astype(int)

        _list_for_df = []
        for _list in df['性別、年齢、属性'].str.replace('（', '').str.replace('）', '').str.split('/'):
            if _list is np.nan:
                _list = ['不明', '不明', '不明']
            elif len(_list) == 1:
                _list.extend(['不明', '不明'])
            elif len(_list) == 2:
                _list.append('不明')
            _list_for_df.append(_list)
            
        df = df.reset_index(drop=True)

        df = pd.concat([df,pd.DataFrame(_list_for_df, columns=['性別', '年齢', '職業'])], axis=1)
        df.drop('性別、年齢、属性', axis=1, inplace=True)

        code_regex = re.compile('[!"#$%&\'\\\\()*+,-./:;<=>?@[\\]^_`{|}~「」〔〕“”〈〉『』【】＆＊・（）＄＃＠。、？！｀＋￥％♪【】【】≪≫、､]')
        _list = []
        # df['駅徒歩'] = df['アクセス'].apply(lambda x: mojimoji.zen_to_han(x))
        df['駅徒歩'] = df['アクセス']
        for row in df['駅徒歩']:
            if '徒歩' in row and '分' in row and '駅' in row:
                row = row.replace('　', '').replace(' ', '')
                row = code_regex.sub('', row)
                row = mojimoji.zen_to_han(row)
                row = code_regex.sub('', row)
                obj = re.search(r'(駅\D*?徒歩\D*\d{1,2}分){1}', row)
                
                # objがあったら文字列を取得して、その中の２桁以下の数字のみを取り出す。数字が
                if obj is not None:
                    row = obj.group()
                    row_list = re.split('\D+', row)
                    for row in row_list:
                        if row != '':
                            update_row = row

                    # 数字が二桁を超えた場合１００００を入れる。
                    if len(update_row) > 2:
                        update_row = 10000

                    update_row = int(update_row)
                            
                # objがない（None）場合、update_rowに１００００を入れる。        
                else:
                    update_row=10000
            else:
                update_row = 10000
            _list.append(update_row)
        _Series = pd.Series(_list)
        df['駅徒歩'] = _Series
        
        df['駅徒歩'] = np.floor(pd.to_numeric(df['駅徒歩'], errors='coerce')).astype('Int64')
        
        
        df['メニューの種類'] = df['メニューの種類'].str.replace(' ', '').str.replace(' ', '').str.replace('[施術メニュー]', '', regex=False)

        df['メニューの種類'] = df['メニューの種類'].fillna('メニュー無し')

        _cut_list=[]
        _color_list=[]
        _treatment_list=[]
        _pama_list=[]
        _straightening_list=[]
        _other_list=[]
        _head_spa_list=[]
        _hair_set_list=[]
        _extension_list=[]
        _dressing_list=[]
        _nothing_list=[]
        for row in df['メニューの種類']:
            
            if 'カット' in row.split('、'):
                cut = 1
            else:
                cut = 0
            _cut_list.append(cut)
            
            if 'カラー' in row.split('、'):
                color=1
            else:
                color=0
            _color_list.append(color)
            
            if 'トリートメント' in row.split('、'):
                treatment=1
            else:
                treatment=0
            _treatment_list.append(treatment)
            
            if 'パーマ' in row.split('、'):
                pama=1
            else:
                pama=0
            _pama_list.append(pama)
            
            if '縮毛矯正' in row.split('、'):
                straightening=1
            else:
                straightening=0
            _straightening_list.append(straightening)
            
            if 'その他' in row.split('、'):
                other=1
            else:
                other=0
            _other_list.append(other)
            
            if 'ヘッドスパ' in row.split('、'):
                head_spa=1
            else:
                head_spa=0
            _head_spa_list.append(head_spa)
                
            if 'ヘアセット' in row.split('、'):
                hair_set=1
            else:
                hair_set=0
            _hair_set_list.append(hair_set)
                
            if 'エクステ' in row.split('、'):
                extension=1
            else:
                extension=0
            _extension_list.append(extension)

            if '着付け' in row.split('、'):
                dressing=1
            else:
                dressing=0
            _dressing_list.append(dressing)
                
            if 'メニュー無し' in row.split('、'):
                nothing=1
            else:
                nothing=0
            _nothing_list.append(nothing)
            
        _df = pd.DataFrame({'カット選択': _cut_list,
                    'カラー選択': _color_list,
                    'トリートメント選択': _treatment_list,
                    'パーマ選択': _pama_list,
                    '縮毛矯正選択': _straightening_list,
                    'その他選択': _other_list,
                    'ヘッドスパ選択': _head_spa_list,
                    'ヘアセット選択': _hair_set_list,
                    'エクステ選択': _extension_list,
                    '着付け選択': _dressing_list,
                    'メニュー無し選択': _nothing_list})

        df = df.reset_index(drop=True)
        df = pd.concat(objs=[df, _df,], axis=1)

        df.dropna(subset=['投稿日時'])
        df = df.dropna(subset=['投稿日時']).reset_index(drop=True)

        mean_of_staff = df.dropna(subset=['スタッフ数'])['スタッフ数'].mean()

        df['スタッフ数'] = df['スタッフ数'].fillna(mean_of_staff)
        df['スタッフ数'] = df['スタッフ数'].astype(int)

        df['イルミナメニュー化の有無'] = 0
            
        illumina_use_judge(df=df, column='1番人気クーポン名')
        illumina_use_judge(df=df, column='2番人気クーポン名')
        illumina_use_judge(df=df, column='3番人気クーポン名')

        for salon_name in df['サロン名']:
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('イルミナ').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('ILLUMINA').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('ｲﾙﾐﾅ').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('illumina').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('Illumina').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('ｉｌｌｕｍｉｎａ').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('Ｉｌｌｕｍｉｎａ').sum():
                df.loc[df['サロン名']==salon_name, 'イルミナメニュー化の有無'] = 1
                
        df['Aujuaメニュー化の有無'] = 0
            
        aujua_use_judge(df=df, column='1番人気クーポン名')
        aujua_use_judge(df=df, column='2番人気クーポン名')
        aujua_use_judge(df=df, column='3番人気クーポン名')

        for salon_name in df['サロン名']:
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('オージュア').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('ｵｰｼﾞｭｱ').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('Aujua').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('aujua').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('ａｕｊｕａ').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1
            if df[df['サロン名']==salon_name]['選択されたクーポン'].str.contains('Ａｕｊｕａ').sum():
                df.loc[df['サロン名']==salon_name, 'Aujuaメニュー化の有無'] = 1

        df['選択されたクーポン_編集'] = df['選択されたクーポン'].apply(lambda x: mojimoji.zen_to_han(x, kana=False))
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].str.replace('　', '').str.replace(' ', '')

        # ひらがな消す。
        p = re.compile('[\u3041-\u309F]+')
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: p.sub('', x))

        # 次にカタカナを消す。
        p2 = re.compile('[\u30A1-\u30FF]+')
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: p2.sub('', x))
        
        # 漢字を消す
        p3 = regex.compile(r'\p{Script_Extensions=Han}+')
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: p3.sub('', x))

        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].str.replace(',', '').str.replace('，', '')

        p4 = re.compile('[a-zA-Z]+')
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: p4.sub('', x))
        
        # \はダメなのでけす
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].str.replace('\\', '', regex=True)
        
        p5 = re.compile('[^0-9]+')
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: p5.split(x))
        
        # ４桁未満を削除する
        _list_for_Series=[]
        for _list in df['選択されたクーポン_編集']:
            _tmp_list=[]
            for row in _list:
                if len(row) >= 4:
                    _tmp_list.append(row)
            _list_for_Series.append(_tmp_list)
        _list_for_Series
        
        df['選択されたクーポン_編集'] = _list_for_Series
        df['選択されたクーポン_編集'] = df['選択されたクーポン_編集'].apply(lambda x: int(x[-1]))

        df.rename(columns={'選択されたクーポン_編集':'支出金額'}, inplace=True)

        # 支出金額が0でかつカットしかしていない顧客はカット料金で埋める。
        df.loc[
               ((df['支出金額']==0)&(df['カット選択']==1)&(df['カラー選択']==0)&(df['トリートメント選択']==0)&(df['パーマ選択']==0)&(df['縮毛矯正選択']==0)&(df['その他選択']==0)&\
                (df['ヘッドスパ選択']==0)&(df['ヘアセット選択']==0)&(df['エクステ選択']==0)&(df['着付け選択']==0)&(df['メニュー無し選択']==0)),
               '支出金額'
                ] =\
        df.loc[
               ((df['支出金額']==0)&(df['カット選択']==1)&(df['カラー選択']==0)&(df['トリートメント選択']==0)&(df['パーマ選択']==0)&(df['縮毛矯正選択']==0)&(df['その他選択']==0)&\
                (df['ヘッドスパ選択']==0)&(df['ヘアセット選択']==0)&(df['エクステ選択']==0)&(df['着付け選択']==0)&(df['メニュー無し選択']==0)),
               'カット料金'
        ]
        
        # nullを０埋めしておく。
        df.fillna(0, inplace=True)
        
        # 重複業を削除
        df = df.drop_duplicates().reset_index(drop=True)
        
        # 
        df.to_csv(save_file_path, index=False)
        logger.info(f'{file_name}の処理が完了しました。(正常終了)')
    
    except Exception:
        logger.error(f'{file_name}でエラーが起きました。')
        save_errordir_path = path.parents[1] / 'データフレームの前処理でエラー'

        if not save_errordir_path.exists():
            save_errordir_path.mkdir(parents=True)
        save_errorfile_path = Path(save_errordir_path, file_name)
        
        error_df = pd.read_csv(path)
        error_df.to_csv(save_errorfile_path, index=False)
        
    
def illumina_use_judge(df, column):
    df.loc[df[column].str.contains('イルミナ'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('ILLUMINA'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('ｲﾙﾐﾅ'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('illumina'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('Illumina'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('ｉｌｌｕｍｉｎａ'), 'イルミナメニュー化の有無'] = 1
    df.loc[df[column].str.contains('Ｉｌｌｕｍｉｎａ'), 'イルミナメニュー化の有無'] = 1

def aujua_use_judge(df, column):
    df.loc[df[column].str.contains('オージュア'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('ｵｰｼﾞｭｱ'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('Aujua'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('AUJUA'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('aujua'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('ａｕｊｕａ'), 'Aujuaメニュー化の有無'] = 1
    df.loc[df[column].str.contains('Ａｕｊｕａ'), 'Aujuaメニュー化の有無'] = 1

def main():
    DIR_PATH = Path('/Users/masudaniwabinari/Desktop/hpb_scraping/data/完成')
    ALL_FILE_PATH = DIR_PATH.glob('*.csv')
    file_path_list = list(ALL_FILE_PATH)
    
    with Pool(8) as p:
        list(tqdm(p.imap_unordered(preprocess_dataframe, file_path_list), total=len(file_path_list)))

if __name__ == '__main__':
    main()