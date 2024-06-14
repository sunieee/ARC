import pandas as pd
import pymysql
import multiprocessing
from datetime import datetime
import re
from collections import defaultdict
import os
from tqdm import tqdm
import math
import json

field = os.environ.get('field', 'graphdrawing')

def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                           user='root',
                           password='root',
                           db=database,
                           charset='utf8')
    return conn, conn.cursor()

# 创建数据库连接
conn, cursor = create_connection()


papers = pd.read_csv(f'out/{field}/papers.csv', dtype={'paperID':str})
papers['PublicationDate'] = pd.to_datetime(papers['PublicationDate'])
papers['year'] = papers['PublicationDate'].apply(lambda x: x.year)
suffix = ''

def extract_paper_authors(pairs):
    papers, info = pairs
    print('extract_paper_authors', len(papers), info)
    conn, cursor = create_connection()
    _paperID2authorsName = defaultdict(list)

    # 使用IN子句一次查询多个paperID
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    cursor.execute(f"""SELECT paper_author{suffix}.paperID, authors{suffix}.name
                       FROM paper_author{suffix} 
                       JOIN authors{suffix} ON paper_author{suffix}.authorID=authors{suffix}.authorID 
                       WHERE paper_author{suffix}.paperID IN ({paper_ids_str})
                       ORDER BY paper_author{suffix}.paperID, paper_author{suffix}.authorOrder;""")
    result = cursor.fetchall()

    # 使用Python代码来组合结果
    for paperID, name in result:
        _paperID2authorsName[paperID].append(name)
    for paperID, names in _paperID2authorsName.items():
        _paperID2authorsName[paperID] = ', '.join(names)
    conn.close()
    return _paperID2authorsName

def valid_venue(venu):
    if venu is None:
        return False
    if venu in ['None', ' ', '', '0']:
        return False
    return True

def extract_paper_venu(papers):
    conn, cursor = create_connection()
    _paperID2venue = {}
    for paperID in tqdm(papers):
        cursor.execute(f"select ConferenceID, JournalID from papers{suffix} where paperID='{paperID}'")
        result = cursor.fetchone()
        # print(result)
        venu = None
        if valid_venue(result[0]):
            cursor.execute("select abbreviation, name from MACG.conferences where conferenceID=%s", (result[0],))
            res = cursor.fetchone()
            if valid_venue(res):
                venu = res[1] + ' (' + res[0] + ')'
        elif valid_venue(result[1]):
            cursor.execute("select name from MACG.journals where journalID=%s", (result[1],))
            res = cursor.fetchone()
            if res != None:
                venu = res[0]
        _paperID2venue[paperID] = venu

    conn.close()
    return _paperID2venue

def extract_paper_abstract(pairs):
    papers, info = pairs
    print('extract_paper_abstract', len(papers), info)
    conn, cursor = create_connection()
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

    cursor.close()
    conn.close()
    return _paperID2abstract


paperID_list = papers["paperID"].values.tolist()
print('len(paperID_list)', len(paperID_list), datetime.now().strftime('%H:%M:%S'))

paperID2venue = defaultdict(str)
paperID2authorsName = defaultdict(str)
multiproces_num = 20
group_size = 2000
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_venu, [paperID_list[i::multiproces_num] for i in range(multiproces_num)])
    for result in results:
        paperID2venue.update(result)
print('finish extract_paper_venu', len(paperID2venue), datetime.now().strftime('%H:%M:%S'))

group_length = math.ceil(len(paperID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_authors, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
    for result in results:
        paperID2authorsName.update(result)
print('finish extract_paper_authors', len(paperID2authorsName), datetime.now().strftime('%H:%M:%S'))

if os.path.exists(f"paperID2abstract.json"):
    with open(f"paperID2abstract.json") as f:
        paperID2abstract = json.load(f)
else:
    paperID2abstract = defaultdict(str)
    group_length = math.ceil(len(paperID_list)/group_size)
    with multiprocessing.Pool(processes=multiproces_num) as pool:
        results = pool.map(extract_paper_abstract, [(paperID_list[i*group_size:(i+1)*group_size], f'{i}/{group_length}') for i in range(group_length)])
        for result in results:
            paperID2abstract.update(result)
    print('finish extract_paper_abstract', len(paperID2abstract))
    with open(f"paperID2abstract.json", 'w') as f:
        json.dump(paperID2abstract, f)


papers['paperID'] = papers['paperID'].astype(str)
papers['isKeyPaper'] = 1
papers['venu'] = papers['paperID'].apply(lambda paperID: paperID2venue[paperID])
papers['authorsName'] = papers['paperID'].apply(lambda paperID: paperID2authorsName[paperID])
papers['abstract'] = papers['paperID'].apply(lambda paperID: paperID2abstract.get(paperID, ''))


papers.to_csv(f'out/{field}/papers.csv', index=False)
papers.describe().to_csv(f'out/{field}/papers_describe.csv')
print("结果已保存到 papers.csv 文件中。")
