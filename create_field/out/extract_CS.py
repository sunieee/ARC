import pymysql
import pandas as pd
from tqdm import tqdm
import json
import re
import os

# 连接数据库
def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    cur = conn.cursor()
    return conn, cur

typ = 1

if typ == 1:
    file = 'fellow'
elif typ == 10:
    file = 'turing'

folder = 'fellowV3'
df = pd.read_csv(f'{folder}/{file}.csv', dtype={'authorID': str})

df['PaperCount'] = 0
df['CitationCount'] = 0
df['CSPaperCount'] = 0
df['CSCitationCount'] = 0
df['hIndex'] = 0

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

reject_subfields = set(['44870925', '1276947', '51244244'])
# new_reject_subfields = set()
# for s in reject_subfields:
#     t = find_all_descendants(s)
#     new_reject_subfields.update(t)
#     print(s, len(t))
# reject_subfields = new_reject_subfields

# os._exit(0)
all_CS_subfields = find_all_descendants('41008148')
print('all CS subfields:', len(all_CS_subfields))

def len_intersection(a, b):
    return len(a.intersection(b))

def has_intersection(a, b):
    return len_intersection(a, b) > 0

def get_hIndex(citations):
    citations = sorted(citations, reverse=True) 
    return sum(1 for i, citation in enumerate(citations) if citation > i)

# 列下在computer science这个level 0领域的总论文数与总引
def calc(rows):
    result = []
    conn, cur = create_connection()
    for row in tqdm(rows):
        query = f"""
            SELECT paperID FROM MACG.paper_author
            WHERE authorID="{row['authorID']}";
        """
        cur.execute(query)
        ret = cur.fetchall()
        ret = [r[0] for r in ret]
        row['PaperCount'] = len(ret)
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
            # l = len_intersection(ret, all_CS_subfields)
            # if len(ret):
            # # if l:
            #     # rate = min(l / len(ret) * 2, 1)
            #     rate = l / len(ret)
            #     # rate = 1
            #     row['CSPaperCount'] += rate
            #     row['CSCitationCount'] += citationCount * rate
            if has_intersection(ret, all_CS_subfields) and not has_intersection(ret, reject_subfields):
                row['CSPaperCount'] += 1
                row['CSCitationCount'] += citationCount
        row['CitationCount'] = sum(citationCounts)
        row['hIndex'] = get_hIndex(citationCounts)
        if row['hIndex'] >= 5:
            result.append(row)
    return result


import multiprocessing as mp
cnt = mp.cpu_count()
with mp.Pool(cnt) as pool:
    rets = pool.map(calc, [df.to_dict('records')[i::cnt] for i in range(cnt)]) 

ret = []
for r in rets:
    ret.extend(r)

df = pd.DataFrame(ret)
df = df[df['CSPaperCount'] != 0]

# df.sort_values(by=['CSPaperCount'], ascending=False, inplace=True)
# df.sort_values(by=['original'], ascending=True, inplace=True)

df.sort_values(by=['original', 'CSPaperCount'], ascending=[True, False], inplace=True)
print(len(df['original'].unique()))

unique_authors = df['original'].unique().tolist()

# dup_authors = []
# for original in unique_authors:
#     if len(df[df['original'] == original]) > 1:
#         dup_authors.append(original)
# df = df[~df['original'].isin(dup_authors)]

df['CSPaperRatio'] = df['CSPaperCount'] / df['PaperCount']
df['CSCitationRatio'] = df['CSCitationCount'] / df['CitationCount']
# 保留4位小数
df['CSPaperRatio'] = df['CSPaperRatio'].apply(lambda x: round(x, 4))
df['CSCitationRatio'] = df['CSCitationRatio'].apply(lambda x: round(x, 4))

df.to_csv(f'{folder}/{file}CS.csv', index=None)
