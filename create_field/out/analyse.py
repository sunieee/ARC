"""
读取 papers.csv，查看有多少比例的会议/期刊论文
"""
import pandas as pd
import os
import time
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

os.environ.setdefault('user', 'root')
os.environ.setdefault('password', 'root')


ids = ['1184914352', '1130985203', '1203999783',  '30698027','196139623']
database = 'MACG'


def create_connection(database=database):
    conn = pymysql.connect(host='localhost',
                            user=os.environ.get('user'),
                            password=os.environ.get('password'),
                            db=database,
                            charset='utf8')
    return conn, conn.cursor()
    
userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
conn, cursor = create_connection()
engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/'+database)


def get_name_by_id(id):
    sql = f"SELECT * FROM MACG.conferences where conferenceID = '{id}'"
    cursor.execute(sql)
    if cursor.rowcount != 0:
        ret = cursor.fetchone()
        return 'ConferenceID', ret[1] + '_' + ret[2]

    sql = f"SELECT * FROM MACG.journals where journalID = '{id}'"
    cursor.execute(sql)
    if cursor.rowcount != 0:
        return 'JournalID', cursor.fetchone()[1]
    
    return None, None


fields = ['AI0', 'AI3']
papers = [pd.read_csv(f'{field}/csv/papers.csv') for field in fields]
papers = pd.concat(papers)

print('before drop duplicate:', papers.shape)
papers.drop_duplicates(subset=['paperID'], inplace=True)
papers['ConferenceID'] = papers['ConferenceID'].apply(lambda x: str(x).replace('.0', ''))
papers['JournalID'] = papers['JournalID'].apply(lambda x: str(x).replace('.0', ''))
print('after drop duplicate:', papers.shape)
print(papers.head(10))


df = pd.DataFrame(columns=['id', 'type', 'name', 'MAG_count', 'field_count', 'ratio'])

for id in ids:
    key, name = get_name_by_id(id)
    print(key, name)
    if key is None:
        continue
    sql = f"SELECT count(*) FROM MACG.papers where {key} = '{id}'"
    cursor.execute(sql)
    MAG_count = cursor.fetchone()[0]
    field_count = papers[papers[key] == id].shape[0]
    ratio = field_count / MAG_count
    df.loc[len(df)] = [id, key, name, MAG_count, field_count, ratio]

df.to_csv('ratio.csv', index=False)