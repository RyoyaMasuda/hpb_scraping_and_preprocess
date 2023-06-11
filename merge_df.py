import pandas as pd
from pathlib import Path

read_dir_path = Path('/Users/masudaniwabinari/Desktop/hpb_scraping/data/データフレームの前処理')
save_dir_path = read_dir_path / 'cocnat'

if not save_dir_path.exists():
    save_dir_path.mkdir(parents=True)
    
_list=[]
for read_file_path in read_dir_path.glob('*.csv'):
    _df = pd.read_csv(read_file_path)
    _list.append(_df)
    
df = pd.concat(_list, axis=0)

file_name = 'merge_df.csv'
save_file_path = save_dir_path / file_name

df.to_csv(save_file_path, index=False)    