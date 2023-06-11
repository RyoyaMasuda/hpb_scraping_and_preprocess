from pathlib import Path

DATA_PATH = Path('./data/完成')
for file_path in DATA_PATH.glob('*.csv'):
    print(file_path.name)
    
    updated_name = file_path.name.replace('の美容院・美容室・ヘアサロン', '')
    print(updated_name)
    file_path.rename(updated_name)