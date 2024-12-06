import pymysql
from sqlalchemy import create_engine
import time
import sys
import os
from tqdm import tqdm
import math
import json
import multiprocessing
from datetime import datetime
import pandas as pd
import re
from collections import defaultdict

if os.environ.get('scholar') == '1':
    fellow = True
    from utils_scholar import *
else:
    fellow = False
    from utils import database, create_top, field, create_connection

    df_authors = pd.read_csv(f"out/{field}/csv/authors.csv")
    df_authors['authorID'] = df_authors['authorID'].astype(str)
    df_paper_author = pd.read_csv(f"out/{field}/csv/paper_author.csv")
    df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
    df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
    df_papers = pd.read_csv(f"out/{field}/csv/papers.csv")
    df_papers['paperID'] = df_papers['paperID'].astype(str)

multiproces_num = 20
print('load data finished', datetime.now().strftime("%H:%M:%S"))
cnt = len(df_authors)
min_size = 3
# filtered_authors = df_authors
# while cnt > max(len(df_authors) * 0.1, 100000):
#     min_size += 1
filtered_authors = df_authors[df_authors['PaperCount_field'] >= min_size]
cnt = len(filtered_authors)

print('min_size:', min_size, 'cnt:', cnt)
authorIDs = set(filtered_authors['authorID'].tolist())

df_paper_author = df_paper_author[df_paper_author['authorID'].isin(authorIDs)]
print('df_paper_author:', len(df_paper_author), datetime.now().strftime("%H:%M:%S"))

df_papers = df_papers[df_papers['paperID'].isin(df_paper_author['paperID'])]
print('df_papers:', len(df_papers), datetime.now().strftime("%H:%M:%S"))

paperID2citationCount = pd.Series(df_papers.citationCount.values, index=df_papers.paperID).to_dict()
print('paperID2citationCount:', len(paperID2citationCount), datetime.now().strftime("%H:%M:%S"))


# 对 df_paper_author 按照 authorID 进行分组，并将 paperID 聚合为列表
def calculate_h_index(paperIDs, paperID2citationCount):
    citations = [paperID2citationCount.get(paperID, 0) for paperID in paperIDs]
    citations.sort(reverse=True)
    h_index = sum(1 for i, citation in enumerate(citations) if citation > i)
    return h_index

author_h_index = df_paper_author.groupby('authorID')['paperID'].apply(lambda paperIDs: calculate_h_index(paperIDs, paperID2citationCount))
print('author_h_index:', len(author_h_index), datetime.now().strftime("%H:%M:%S"))

authorID2h_index = author_h_index.to_dict()
with open(f'out/{field}/authorID2h_index.json', 'w') as f:
    json.dump(authorID2h_index, f)

df_authors['hIndex_field'] = df_authors['authorID'].apply(lambda authorID: authorID2h_index.get(authorID, 0))
df_authors.to_csv(f'out/{field}/csv/authors.csv', index=False)


if fellow:
    df_paper_author_filtered = df_paper_author[df_paper_author['authorID'].isin(authorIDs)]
else:
    conn, cursor = create_connection(database)
    for authorID, h_index in tqdm(authorID2h_index.items()):
        cursor.execute(f"UPDATE authors_field SET hIndex_field = {h_index} WHERE authorID = '{authorID}'")
    conn.commit()
    _, _, _, df_paper_author_filtered, _ = create_top()
    

paperIDs = set(df_paper_author_filtered['paperID'].drop_duplicates().tolist())
papers_top = pd.read_csv(f'out/{field}/csv/papers.csv')
papers_top['paperID'] = papers_top['paperID'].astype(str)
papers_top = papers_top[papers_top['paperID'].isin(paperIDs)]
papers_top = papers_top[['paperID', 'title']]
print('papers_top count', len(papers_top))

def extract_paper_abstract(pairs):
    papers, info = pairs
    print('extract_paper_abstract', len(papers), info)
    conn = pymysql.connect(host='localhost',
                            port=3306,
                            user=os.environ.get('user'),
                            password=os.environ.get('password'),
                            db='MACG',
                            charset='utf8')
    
    combineOpenAlex = True
    if combineOpenAlex:
        conn1 = pymysql.connect(host='localhost',
                                port=3306,
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db='openalex',
                                charset='utf8')
        cursor1 = conn1.cursor()
        
    cursor = conn.cursor()
    _paperID2abstract = defaultdict(str)

    # 使用IN子句一次查询多个paperID
    # 这个太重要了！！！！！！！ paperID一定要加引号，不然慢1w倍，1s变成10h
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    sql = f"""SELECT paperID, abstract FROM abstracts WHERE paperID IN ({paper_ids_str}) ;"""
    # print('*', sql)
    cursor.execute(sql)
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for paperID, abstract in result:
        _paperID2abstract[paperID] = re.sub('\s+', ' ', abstract)
        if combineOpenAlex and not abstract:
            cursor1.execute(f"SELECT abstract FROM abstracts WHERE paperID = '{paperID}'")
            result = cursor1.fetchone()
            if result:
                _paperID2abstract[paperID] = re.sub('\s+', ' ', result[0])
            else:
                _paperID2abstract[paperID] = ''

    cursor.close()
    conn.close()
    return _paperID2abstract

if os.path.exists(f"out/{field}/paperID2abstract.json"):
    with open(f"out/{field}/paperID2abstract.json") as f:
        paperID2abstract = json.load(f)
else:
    paperID2abstract = defaultdict(str)
    multiproces_num = 20
    group_size = 2000
    group_length = math.ceil(len(paperIDs)/group_size)
    paperID_list = list(paperIDs)
    with multiprocessing.Pool(processes=multiproces_num * 2) as pool:
        results = pool.map(extract_paper_abstract, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
        for result in results:
            paperID2abstract.update(result)
    print('finish extract_paper_abstract', len(paperID2abstract))
    with open(f"out/{field}/paperID2abstract.json", 'w') as f:
        json.dump(paperID2abstract, f)

papers_top['abstract'] = papers_top['paperID'].apply(lambda x: paperID2abstract.get(x, ''))
papers_top.to_csv(f'out/{field}/papers_top.csv', index=False)