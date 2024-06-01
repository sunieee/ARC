"""
## 建立作者GF图
"""

import pymysql
import pandas as pd
from tqdm import tqdm
import json
import re

typ = 1 # ACMFellow: https://awards.acm.org/fellows/award-winners
# typ = 10 # A.M. Turing Award: https://amturing.acm.org/byyear.cfm
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


def format_name(name):
    name = name.strip('* .')
    name = re.sub(r"\s*\([^)]*\)", "", name).strip()
    parts = name.split(',')
    if len(parts) == 1:
        return name
    a, b = parts
    c = ''
    for ix, t in enumerate(b.split()):
        if ix >= 1:
            c += t[0] + '. '
        elif len(t) == 1:
            c += t + '. '
        else:
            c += t + ' '
    return c + a
    

#####################################################3
# 1. 读取网页获奖者名单
original2year = {}
if typ == 1:
    with open(f'fellow.txt') as f:
        for line in f.read().strip().split('\n'):
            name = format_name(line.split('ACM Fellows')[0])
            year = int(line.split('ACM Fellows')[-1].strip().split()[0])
            original2year[name] = year
elif typ == 10:
    with open(f'turing.txt') as f:
        lines = f.read().strip().split('\n')
    for i in range(0, len(lines)):
        line = lines[i].strip()
        if line.startswith('(') and line.endswith(')'):
            year = int(line.strip("()"))  # Extracting the year
        else:
            name = format_name(line)
            original2year[name] = year

print('original2year:', original2year)

############################################################
# 2. 读取数据集获奖者
award_df = pd.read_csv(f'{folder}/award_authors.csv')
award_df = award_df[award_df['type'] == typ]
award_df['MAGID'] = award_df['MAGID'].astype(str).apply(lambda x: x.split('.')[0])
ids = award_df['MAGID'].unique()
print('valid MAGID in award_authors.csv', len(ids))


###########################################################
# 3. （使用网页数据）查询并选出PaperCount最高的3个且大于10的ACM Fellows

def remove_middle_name(name):
    parts = name.split()
    if len(parts) == 2:
        return name
    return parts[0] + ' ' + parts[-1]

def short2_name(name):
    parts = name.split()
    return parts[0][:2] + ' ' + parts[-1]

def short3_name(name):
    parts = name.split()
    return parts[0][:3] + ' ' + parts[-1]

id2year = {}
id2original = {}

def query(names):
    conn, cur = create_connection()
    name2ret = {}
    for name in tqdm(names):
        query = f"""
            SELECT * FROM MACG.authors
            WHERE name in ("{name}", "{remove_middle_name(name)}", "{short2_name(name)}", "{short3_name(name)}") 
            AND CitationCount >= 200
            ORDER BY CitationCount desc;
        """
        cur.execute(query)
        ret = cur.fetchall()
        # choose top 3
        ret = list(ret)

        if len(name.split()) == 2:
            pattern = name.split()[0] + ' %% ' + name.split()[-1]
            query = f"""
                SELECT * FROM MACG.authors
                WHERE name like "{pattern}"
                AND CitationCount >= 200
                ORDER BY CitationCount desc;
            """
            cur.execute(query)
            ret2 = cur.fetchall()
            ret2 = list(ret2)
            ret.extend(ret2)
        else:
            pattern = name.split()[0] + ' ' + name.split()[1][0] + '%% ' + name.split()[-1]
            query = f"""
                SELECT * FROM MACG.authors
                WHERE name like "{pattern}"
                AND CitationCount >= 200
                ORDER BY CitationCount desc;
            """
            cur.execute(query)
            ret2 = cur.fetchall()
            ret2 = list(ret2)
            ret.extend(ret2)

        if len(ret) > 0:            
            name2ret[name] = ret
    
    return name2ret


import multiprocessing as mp
cnt = mp.cpu_count()
with mp.Pool(cnt) as pool:
    rets = pool.map(query, [list(original2year.keys())[i::cnt] for i in range(cnt)])

results = []
name2ret = {}
for ret in rets:
    name2ret.update(ret)
    
valid_names = list(name2ret.keys())
for name, ret in name2ret.items():
    results.extend(ret)
    for row in ret:
        id2year[str(row[0])] = original2year[name]
        id2original[str(row[0])] = name


############################################################
# 4. 添加数据集数据，获取所有作者信息
print('len(valid_names):', len(valid_names))
id2year.update(zip(award_df['MAGID'], award_df['year']))
name2id = {}
conn, cur = create_connection()
for id in tqdm(ids):
    query = f"""
        SELECT * FROM MACG.authors
        WHERE authorID="{id}";
    """
    # print(query)
    cur.execute(query)
    ret = cur.fetchall()
    results.extend(ret)
    if len(ret) > 0:
        name2id[ret[0][2]] = id
conn.close()

########################################################
# 5. 创建DataFrame并显示结果
df = pd.DataFrame(results, columns=['authorID', 'rank', 'name', 'PaperCount', 'CitationCount'])
df['authorID'] = df['authorID'].astype(str)
df['CitationCount'] = df['CitationCount'].astype(int)
df['PaperCount'] = df['PaperCount'].astype(int)
df['original'] = df['authorID'].apply(lambda x: id2original.get(x, ''))
df['year'] = df['authorID'].apply(lambda x: id2year.get(x, 0))
df = df[['original', 'authorID', 'name', 'PaperCount', 'CitationCount', 'year']]
original2year = {k: v for k, v in original2year.items() if k not in valid_names}
for k, v in original2year.items():
    print(k, v)
    # df.loc[len(df)] = [k, '', '', 0, 0, v]
    df = df.append({'original': k, 'authorID': '', 'name': '', 'PaperCount': 0, 'CitationCount': 0, 'year': v}, ignore_index=True)

# df.sort_values(by=['original'], inplace=True, ascending=True)
for name in name2id.keys():
    df.drop(df[(df['name'] == name) & (df['authorID'] != name2id[name])].index, inplace=True)

# df.drop_duplicates(subset=['authorID'], keep='first', inplace=True)
df_empty_authorID = df[df['authorID'] == '']
df_without_empty_authorID = df[df['authorID'] != ''].copy()
df_without_empty_authorID.drop_duplicates(subset=['authorID'], keep='first', inplace=True)
df = pd.concat([df_without_empty_authorID, df_empty_authorID])

# name重复，则保留PaperCount最大且CitationCount最大的行，删掉PaperCount和CitationCount不是最大的行
# 定义一个函数来检查每个分组
def filter_group(group):
    # 检查PaperCount和CitationCount是否有记录同时是最大的
    max_paper = group['PaperCount'].max()
    max_citation = group['CitationCount'].max()
    # 如果存在这样的记录，返回这条记录
    if any((group['PaperCount'] == max_paper) & (group['CitationCount'] == max_citation)):
        return group[(group['PaperCount'] == max_paper) & (group['CitationCount'] == max_citation)]
    # 否则返回空DataFrame
    return pd.DataFrame()

# 暂时不用filter，取paperCount前三的作者
# df = df.groupby('name').apply(filter_group).reset_index(drop=True)
# df.sort_values(by=['name', 'PaperCount', 'CitationCount'], inplace=True, ascending=False)
# df.drop_duplicates(subset=['name'], keep='first', inplace=True)

with open(f'{folder}/not_found_name{typ}.json', 'w') as f:
    json.dump(original2year, f)

if typ == 1:
    df.to_csv(f'{folder}/fellow.csv', index=False)
elif typ == 10:
    df.to_csv(f'{folder}/turing.csv', index=False)

award_df = pd.DataFrame(columns=['original_author_name', 'year', 'type', 'MAGID', 'ARCID'])
for row in df.iterrows():
    row = row[1]
    if row['authorID'] not in ids:
        award_df.loc[len(award_df)] =[row['name'], row['year'], typ, row['authorID'], 'NULL']

award_df.to_csv(f'{folder}/award_authors_add{typ}.csv', index=False)
