
import pymysql
import pandas as pd
import os
import time
from tqdm import tqdm
import multiprocessing
from collections import defaultdict
import math
from datetime import datetime
import json
import re


field = os.environ.get('field')
database = f'v2_{field}_field'
if os.environ.get('scholar') == '1':
    database = os.environ.get('database', 'MACG')

def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()

paper_dir = f'out/{field}/papers_raw'
file_list = os.listdir(paper_dir)

df_papers = pd.read_csv(f'out/{field}/csv/papers.csv')
df_papers['paperID'] = df_papers['paperID'].astype(str)
if 'referenceCount' not in df_papers.columns:
    df_papers['referenceCount'] = -1

if os.path.exists(f'out/{field}/top_authors.csv'):
    top_authors = pd.read_csv(f'out/{field}/top_authors.csv')
else:
    top_authors = pd.read_csv(f'out/{field}/authors.csv')
top_authors['authorID'] = top_authors['authorID'].astype(str)

authorID_list = top_authors['authorID'].tolist()
df_paper_author = pd.read_csv(f"out/{field}/csv/paper_author.csv")
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)

df_paper_author_filtered = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
paperID_list = df_paper_author_filtered['paperID'].drop_duplicates().tolist()
paperID2referenceCount = dict(zip(df_papers['paperID'], df_papers['referenceCount']))
paperID2citationCount = dict(zip(df_papers['paperID'], df_papers['citationCount']))


with open(f"out/{field}/paperID2abstract.json") as f:
    paperID2abstract = json.load(f)

def extract_paper_authors(pairs):
    papers, info = pairs
    print('extract_paper_authors', len(papers), info)
    conn, cursor = create_connection(database)
    _paperID2authorsName = defaultdict(list)

    # 使用IN子句一次查询多个paperID
    paper_ids_str = ', '.join([f"'{x}'" for x in papers])
    cursor.execute(f"""SELECT paper_author.paperID, authors.name
                       FROM paper_author
                       JOIN authors ON paper_author.authorID=authors.authorID 
                       WHERE paper_author.paperID IN ({paper_ids_str})
                       ORDER BY paper_author.paperID, paper_author.authorOrder;""")
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
    if field.count("acl_anthology"):
        return {}
    conn, cursor = create_connection(database)
    _paperID2venue = {}
    for paperID in tqdm(papers):
        cursor.execute(f"select ConferenceID, JournalID from papers where paperID='{paperID}'")
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

paperID2venue = defaultdict(str)
paperID2authorsName = defaultdict(str)

paperID_list = []
for file in tqdm(file_list):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    papers['paperID'] = papers['paperID'].astype(str)
    paperID_list += papers["paperID"].values.tolist()
paperID_list = list(set(paperID_list))
print('len(paperID_list)', len(paperID_list), datetime.now().strftime('%H:%M:%S'))

multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_venu, [paperID_list[i::multiproces_num] for i in range(multiproces_num)])
    for result in results:
        paperID2venue.update(result)
print('finish extract_paper_venu', len(paperID2venue), datetime.now().strftime('%H:%M:%S'))

group_size = 2000
group_length = math.ceil(len(paperID_list)/group_size)
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper_authors, [(paperID_list[i*group_size:i*group_size+group_size], f'{i}/{group_length}') for i in range(group_length)])
    for result in results:
        paperID2authorsName.update(result)
print('finish extract_paper_authors', len(paperID2authorsName), datetime.now().strftime('%H:%M:%S'))

def extract_paper(file):
    filepath = os.path.join(paper_dir, file)
    papers = pd.read_csv(filepath)
    for col in ["authorOrder", "firstAuthorID", "firstAuthorName"]:
        if col in papers.columns:
            papers = papers.drop(columns=[col])
    papers['paperID'] = papers['paperID'].astype(str)
    papers['referenceCount'] = papers['paperID'].apply(lambda paperID: paperID2referenceCount.get(paperID, 0))
    papers['citationCount'] = papers['paperID'].apply(lambda paperID: paperID2citationCount.get(paperID, 0))
    papers['venu'] = papers['paperID'].apply(lambda paperID: paperID2venue[paperID])
    papers['authorsName'] = papers['paperID'].apply(lambda paperID: paperID2authorsName[paperID])
    papers['abstract'] = papers['paperID'].apply(lambda paperID: paperID2abstract.get(paperID, ''))

    papers.to_csv(f'out/{field}/papers/' + filepath.split('/')[-1], index=False)

    core_papers = papers[papers['isKeyPaper'] > 0.5]
    core_citations = core_papers['citationCount'].to_list()
    core_citations.sort(reverse=True)

    citations = papers['citationCount'].to_list()
    citations.sort(reverse=True)
    return {
        'authorID': file.split('.')[0],
        'hIndex': sum(1 for i, citation in enumerate(citations) if citation > i),
        'CorePaperCount': len(core_papers),
        'CoreCitationCount': core_papers['citationCount'].sum(),
        'CorehIndex': sum(1 for i, citation in enumerate(core_citations) if citation > i),
        'PaperCount': len(papers),
        'CitationCount': papers['citationCount'].sum(),
    }

os.makedirs(f'out/{field}/papers', exist_ok=True)
multiproces_num = 20
with multiprocessing.Pool(processes=multiproces_num) as pool:
    results = pool.map(extract_paper, file_list)

df = pd.DataFrame(results)
# remove columns in top_authors if exist: ['CorePaperCount', 'CoreCitationCount', 'CorehIndex']
cols = ['CorePaperCount', 'CoreCitationCount', 'CorehIndex', 'PaperCount', 'CitationCount', 'hIndex']
for col in cols + [x + '_field' for x in cols]:
    if col in top_authors.columns:
        top_authors = top_authors.drop(columns=[col])
top_authors = top_authors.merge(df, on='authorID')

top_authors.to_csv(f'out/{field}/top_field_authors.csv', index=False)