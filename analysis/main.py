import pandas as pd
import os
import pymysql
import time
from sqlalchemy import create_engine
import sqlalchemy
from collections import defaultdict
import pandas as pd
import multiprocessing
import json
from datetime import datetime
from tqdm import tqdm
import math

database = os.environ.get('database', 'scigene_acl_anthology')

os.makedirs(f'out/{database}', exist_ok=True)
df = pd.read_csv(f'../post_processing/out/{database}/top_field_authors.csv')
df['authorID'] = df['authorID'].astype(str)
authorID_list = df['authorID'].tolist()
print('authorID_list:', len(authorID_list), datetime.now().strftime('%H:%M:%S'))

engine = create_engine(f"mysql+pymysql://root:root@localhost:3306/{database}?charset=utf8")

def init_db(database):
    conn = pymysql.connect(host='localhost',
                                port=3306,
                                user='root',
                                password='root',
                                db=database,
                                charset='utf8')
    cursor = conn.cursor()
    return conn, cursor

def extract_author_fellow(pairs):
    authors, info = pairs
    print('extract_author_fellow', len(authors), info)
    conn, cursor = init_db(database)
    _authorID2fellow = {}

    # 使用IN子句一次查询多个paperID
    author_ids_str = ', '.join([f"'{x}'" for x in authors])
    cursor.execute(f"""SELECT authorID, FellowType
                       FROM authors_ARC 
                       WHERE authorID IN ({author_ids_str});""")
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for authorID, name in result:
        if name:
            _authorID2fellow[authorID]=name.replace('"', '')

    cursor.close()
    conn.close()
    return _authorID2fellow

# if os.path.exists(f'out/{database}/authorID2fellow.json'):
#     with open(f'out/{database}/authorID2fellow.json', 'r') as f:
#         authorID2fellow = json.load(f)
# else:
authorID2fellow = extract_author_fellow((authorID_list, ''))
with open(f'out/{database}/authorID2fellow.json', 'w') as f:
    json.dump(authorID2fellow, f, indent=4, sort_keys=True, ensure_ascii=False)

df['fellow'] = df['authorID'].apply(lambda x: authorID2fellow.get(x, ''))
df.to_csv(f'out/{database}/top_field_authors.csv', index=False)


def to_number(x):
    try:
        return float(x)
    except:
        return 0.0

def process(authorID_list):
    for authorID in tqdm(authorID_list):
        nodes = {}
        edges = []
        # print(authorID, authorID2fellow.get(authorID, ''))
        links_df = pd.read_csv(f'../post_processing/out/{database}/links/{authorID}.csv')

        # 遍历links_df 的每一行
        for index, row in links_df.iterrows():
            edges.append({
                'source': row['childrenID'],
                'target': row['parentID'],
                'prob': to_number(row['extendsProb'])
            })

        papers_df = pd.read_csv(f'../post_processing/out/{database}/papers/{authorID}.csv')

        # 遍历papers_df 的每一行
        for index, row in papers_df.iterrows():
            nodes[row['paperID']] = float(row['isKeyPaper'])

        with open(f'out/{database}/{authorID}.json', 'w') as f:
            json.dump({
                'nodes': nodes,
                'edges': edges,
                'fellow': authorID2fellow.get(authorID, '')
            }, f, indent=4, sort_keys=True, ensure_ascii=False)


multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(process, [authorID_list[i::multiproces_num] for i in range(multiproces_num)])
