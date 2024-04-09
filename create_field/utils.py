from sqlalchemy import create_engine
import pymysql
import time
import json
import numpy as np
from datetime import datetime

import sys
import os
import yaml
import pandas as pd


field = os.environ.get('field')
database = f'scigene_{field}_field'

if os.path.exists(f'yaml/{field}.yaml'):
    with open(f'yaml/{field}.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
else:
    with open('yaml/config.yaml') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
field_info = config[database]
for t in field_info.get('meta', []):
    for k, v in t.items():
        field_info[k] = v

topN = int(field_info.get('topScholarNum', 5000))
print('topN:', topN)

def create_connection(database=database):
    conn = pymysql.connect(host='localhost',
                            user=os.environ.get('user'),
                            password=os.environ.get('password'),
                            db=database,
                            charset='utf8')
    return conn, conn.cursor()


def init_connection(database):
    try:
        return create_connection(database)
    except:
        # Connect to the MySQL server without selecting a database
        conn = pymysql.connect(host='localhost', user=os.environ.get('user'), password=os.environ.get('password'))
        cursor = conn.cursor()
        cursor.execute(f"SHOW DATABASES LIKE '{database}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {database}")
        conn.commit()

        return create_connection(database)


userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
conn, cursor = init_connection(database)
engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/'+database)


def execute(sql):
    for _sql in sql.split(';'):
        _sql = _sql.strip()
        if _sql == '':
            continue
        print('*', _sql)
        t = time.time()
        cursor.execute(_sql)
        conn.commit()
        print('* time:', time.time()-t)

def try_execute(sql):
    try:
        cursor.execute(sql)
    except:
        pass
    conn.commit()

def executeFetch(sql):
    sql = sql.strip()
    t = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    print('[time cost: ', time.time()-t, ']')
    return rows

class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for np types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        return json.JSONEncoder.default(self, obj)
    

def create_top():
    path_to_mapping = f"out/{field}/csv"
    df_authors = pd.read_csv(f"{path_to_mapping}/authors.csv")
    top_authors_path = f'out/{field}/top_authors.csv'
    if not os.path.exists(top_authors_path):
        df_authors['authorID'] = df_authors['authorID'].astype(str)
        df_authors = df_authors.sort_values(by='hIndex_field', ascending=False).reset_index(drop=True)
        # 获取特定位置（假设为变量 'num'）的 hIndex_field 值
        hIndex0 = df_authors.loc[topN, 'hIndex_field']
        assert hIndex0 > 5
        top_authors = df_authors[df_authors['hIndex_field'] >= hIndex0]
        top_authors.to_csv(top_authors_path, index=False)
    else:
        top_authors = pd.read_csv(top_authors_path)
        top_authors['authorID'] = top_authors['authorID'].astype(str)
    authorIDs = set(top_authors['authorID'].tolist())

    print('loading data from dataset', datetime.now().strftime("%H:%M:%S"))
    df_paper_author = pd.read_csv(f"{path_to_mapping}/paper_author.csv")
    df_papers = pd.read_csv(f"{path_to_mapping}/papers.csv")
    
    df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
    df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
    df_papers['paperID'] = df_papers['paperID'].astype(str)
    
    df_papers['PublicationDate'] = pd.to_datetime(df_papers['PublicationDate'])
    df_papers['year'] = df_papers['PublicationDate'].apply(lambda x: x.year)
        
    df_paper_author_filtered = df_paper_author[df_paper_author['authorID'].isin(authorIDs)]
    df_paper_author_filtered = df_paper_author_filtered[['authorID', 'paperID', 'authorOrder']].drop_duplicates()

    if not os.path.exists(f'out/{field}/df_paper_author_filtered.csv'):
        df_paper_author_filtered.to_csv(f'out/{field}/df_paper_author_filtered.csv', index=False)

    if not os.path.exists(f'out/{field}/authorID2name.json'):
        authorID2name = df_authors.set_index('authorID')['name'].to_dict()
        with open(f'out/{field}/authorID2name.json', 'w') as f:
            json.dump(authorID2name, f)
        
    if not os.path.exists(f'out/{field}/paperID2FirstAuthorID.json'):
        paperID2FirstAuthorID = df_paper_author[df_paper_author['authorOrder'] == 1].set_index('paperID')['authorID'].to_dict()
        with open(f'out/{field}/paperID2FirstAuthorID.json', 'w') as f:
            json.dump(paperID2FirstAuthorID, f)

    return df_papers, df_authors, df_paper_author, df_paper_author_filtered, top_authors