"""
已选出fellow 1200人

对于每个人，都在所有候选领域（按比例选取）的top_field_authors中找出1个不重复的
PaperCount, CitationCount近似但非fellow的人，构建他们的GF图。

候选数据库：
- database
- VCG
- SE
- HCI
- CN
- AI

"""
import pymysql
import pandas as pd
from tqdm import tqdm
import json
import re
import numpy as np
import os

folder = 'fellows'

# 连接数据库
def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    cur = conn.cursor()
    return conn, cur

"""
在读取csv，和从数据库中读的时候，一定要把类型转换成str，否则会出现很多问题
后面fellow和non-fellow一个是int一个是str，不匹配
"""
fellow_df = pd.read_csv(f'{folder}/fellowCS_unique.csv', dtype={'authorID': str})
# for col in ['CSPaperCount', 'CSCitationCount', 'PaperCount']:
#     if col in fellow_df.columns:
#         fellow_df.drop(col, axis=1, inplace=True)
# fellow_df.to_csv('fellow.csv', index=False)

fellowIDs = set(fellow_df['authorID'].to_list())

PaperCounts = fellow_df['PaperCount'].to_list()
CitationCounts = fellow_df['CitationCount'].to_list()
hIndexs = fellow_df['hIndex'].to_list()

X = np.array([PaperCounts, CitationCounts, hIndexs]).T
cov_matrix = np.cov(X, rowvar=False)

print('cov_matrix:', cov_matrix)

def mahalanobis_distance(point1, point2):
    d = point1 - point2
    inv_cov_matrix = np.linalg.inv(cov_matrix)
    distance = np.sqrt(np.dot(np.dot(d.T, inv_cov_matrix), d))
    return distance


candidate_databases = [
    'database',
    'VCG',
    'SE',
    'HCI',
    'CN',
    'AI'
]

def find_all_descendants(parent_id):
    conn, cur = create_connection()
    descendants = set()
    
    def recurse(parent):
        query = f"""
            SELECT childrenID FROM MACG.field_children
            WHERE parentID='{parent}';
        """
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        for row in rows:
            child_id = row[0]
            if child_id not in descendants:  # To avoid infinite loops in case of circular references
                descendants.add(child_id)
                recurse(child_id)
        cursor.close()
    
    recurse(parent_id)
    return descendants

all_CS_subfields = find_all_descendants('41008148')
print('all CS subfields:', len(all_CS_subfields))

def len_intersection(a, b):
    return len(a.intersection(b))

def get_hIndex(citations):
    citations = sorted(citations, reverse=True) 
    return sum(1 for i, citation in enumerate(citations) if citation > i)

compareAuthorIDMap = {}

# 列下在computer science这个level 0领域的总论文数与总引
def calc(authorIDs):
    result = []
    conn, cur = create_connection()
    for authorID in tqdm(authorIDs):
        row = {'authorID': authorID}
        query = f"""
            SELECT paperID FROM MACG.paper_author
            WHERE authorID="{authorID}";
        """
        cur.execute(query)
        ret = cur.fetchall()
        ret = [r[0] for r in ret]
        row['PaperCount'] = len(ret)
        row['CSCitationCount'] = 0
        row['CSPaperCount'] = 0
        citationCounts = []

        for paperID in ret:
            query = f"""
                SELECT fieldID FROM MACG.papers_field
                WHERE paperID="{paperID}";
            """
            cur.execute(query)
            ret = cur.fetchall()
            ret = set([str(r[0]) for r in ret])

            query = f"""
                SELECT CitationCount FROM MACG.papers
                WHERE paperID="{paperID}";
            """
            cur.execute(query)
            citationCount = cur.fetchall()[0][0]
            citationCounts.append(citationCount)
            l = len_intersection(ret, all_CS_subfields)

            if len(ret):
                rate = min(l / len(ret) * 2, 1)
                row['CSPaperCount'] += rate
                row['CSCitationCount'] += citationCount * rate

        row['CitationCount'] = sum(citationCounts)
        row['hIndex'] = get_hIndex(citationCounts)
        row['CSPaperRatio'] = round(row['CSPaperCount'] / row['PaperCount'], 4)
        row['CSCitationRatio'] = round(row['CSCitationCount'] / row['CitationCount'], 4)
        if row['CSPaperRatio'] > 0.8:   # 更严格
            result.append(row)
    return result


field2candidateIDs = {}
field2candidate_df = {}
conn, cur = create_connection()
for field in tqdm(candidate_databases):
    if os.path.exists(f'{field}_candidates.csv'):
        field2candidate_df[field] = pd.read_csv(f'{field}_candidates.csv', dtype={'authorID': str})
        field2candidate_df[field]['CSPaperRatio'] = field2candidate_df[field]['CSPaperRatio'].apply(lambda x: round(x, 4))
        field2candidate_df[field]['CSCitationRatio'] = field2candidate_df[field]['CSCitationRatio'].apply(lambda x: round(x, 4))
        field2candidateIDs[field] = set(field2candidate_df[field]['authorID'])
        print('field:', field, 'len:', len(field2candidateIDs[field]), 'candidate:', len(field2candidateIDs[field]))
        continue

    field_authorIDs = set(pd.read_csv(f'../{field}/top_field_authors.csv', dtype={'authorID': str})['authorID'])
    # candidate不能是fellow
    field2candidateIDs[field] = list(field_authorIDs - fellowIDs)
    print('field:', field, 'len:', len(field2candidateIDs[field]), 'candidate:', len(field2candidateIDs[field]))
    import multiprocessing as mp
    cnt = mp.cpu_count()
    with mp.Pool(cnt) as pool:
        rets = pool.map(calc, [field2candidateIDs[field][i::cnt] for i in range(cnt)])
    
    ret = []
    for r in rets:
        ret.extend(r)

    field2candidate_df[field] = pd.DataFrame(ret)
    field2candidate_df[field]['authorID'] = field2candidate_df[field]['authorID'].astype(str) 
    field2candidate_df[field].to_csv(f'{field}_candidates.csv', index=False)

candidate_df = pd.concat([field2candidate_df[field] for field in candidate_databases])
candidate_df.to_csv('candidates.csv', index=False)
total_candidates = len(candidate_df)
non_fellow_df = []

print(field2candidate_df)


for row in tqdm(fellow_df.to_dict(orient='records')):
    current_df = candidate_df.copy()
    current_df['distance'] = current_df.apply(
        lambda x: mahalanobis_distance(np.array([x['PaperCount'], x['CitationCount'], x['hIndex']]),
                                        np.array([row['PaperCount'], row['CitationCount'], row['hIndex']]))
                                        , axis=1)
    current_df = current_df[~current_df['authorID'].isin(fellowIDs)]
    current_df = current_df.sort_values('distance').head(1)

    # add current_df authorID to fellowIDs
    for v in current_df['authorID'].values:
        fellowIDs.add(v)
        compareAuthorIDMap[v] = row['authorID']
        compareAuthorIDMap[row['authorID']] = v
    non_fellow_df.append(current_df)

def search_name(authorID):
    sql = f"select name from authors where authorID = '{authorID}'"
    cur.execute(sql)
    res = cur.fetchone()
    if res:
        return res[0]
    return None
    
non_fellow_df = pd.concat(non_fellow_df)
non_fellow_df['compareAuthorID'] = non_fellow_df['authorID'].apply(lambda x: compareAuthorIDMap[x])
non_fellow_df['name'] = non_fellow_df['authorID'].apply(search_name)
non_fellow_df.to_csv(f'non_fellow.csv', index=False)

fellow_df['fellow'] = True
non_fellow_df['fellow'] = False
fellow_df['field'] = 'fellow'
author_df = pd.concat([fellow_df, non_fellow_df])
author_df.fillna(0, inplace=True)
author_df['year'] = author_df['year'].astype(int)
author_df['compareAuthorID'] = author_df['compareAuthorID'].astype(str)

author_df.to_csv(f'{folder}/authors.csv', index=False) 