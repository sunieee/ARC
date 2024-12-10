import pandas as pd
import numpy as np
import sys
from sqlalchemy import create_engine
from tqdm import tqdm
import os
import time
import sqlalchemy
import concurrent.futures
import multiprocessing
import json
from utils import field, execute, cursor, conn, engine, field_info, try_execute
from datetime import datetime

databaseMAG = os.environ.get('database', 'MACG')
userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
print(f'[extract_scigene_field] databaseMAG: {databaseMAG}, userpass: {userpass}')
GROUP_SIZE = 2000
multiproces_num = 20

paper_path = f'out/{field}/papers.txt'
papers = list(np.loadtxt(paper_path, dtype=str))

####################################################################################
# get_data_from_table
# 从mysql中获取papers, paer_auther, paper_reference, authors四个表的field子数据，并保存到本地文件
####################################################################################

def get_data_from_table_concurrent(table_name, key='paperID', data=papers, database=databaseMAG):
    print(f'# Getting {table_name}({key}) from {database}', datetime.now().strftime('%H:%M:%S'))
    t = time.time()
    db = pd.DataFrame()

    def _query(pair):
        MAG_group, index, pbar = pair
        engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/{database}')
        sql=f'''select * from {table_name} where '''\
              + key + ' in ('+','.join([f'\'{x}\'' for x in MAG_group])+')'
        # time = datetime.now().strftime('%H:%M:%S')
        # print(f"* {time} Executing query for param {index+1}/{len(query_params)} in {table_name}({key})")
        ret = pd.read_sql_query(sql, engine)
        engine.dispose()

        pbar.n = int(index)
        pbar.refresh()
        return ret
    
    group_num = len(range(0, len(data), GROUP_SIZE))
    pbar = tqdm(total=group_num)
    params = [(data[i*GROUP_SIZE:(i+1)*GROUP_SIZE], i, pbar) for i in range(group_num)]
    print(f'## create params in {table_name}({key}), length: {group_num}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num * 10) as executor:
        results = executor.map(_query, params)

    # 将所有结果合并
    # for i in tqdm(range(len(results))):
    #     db=pd.concat([db, results[i]])
    st = time.time()
    db = pd.concat(results)
    print(f'## finish reading {table_name}({key}), merge time cost: {time.time()-st}')

    print(f'{table_name}({key}) original', db.shape)
    db=db.drop_duplicates()
    print(f'{table_name}({key}) drop_duplicates', db.shape, f'time cost: {time.time()-t}')
    return db

print('# getting papers from MAG', datetime.now().strftime('%H:%M:%S'))
df_papers = get_data_from_table_concurrent('papers')
df_papers.to_csv(f'out/{field}/csv/papers.csv',index=False)

df_paper_author = get_data_from_table_concurrent('paper_author')
authors=df_paper_author['authorID'].drop_duplicates().values
df_paper_author.to_csv(f'out/{field}/csv/paper_author.csv',index=False)

combineOpenAlex = True
if combineOpenAlex:
    citing_db = get_data_from_table_concurrent('paper_reference', key='citingpaperID', database='MACG')
    citing_db1 = get_data_from_table_concurrent('paper_reference_no_duplicate', key='citingpaperID', database='openalex')
    cited_db = get_data_from_table_concurrent('paper_reference', key='citedpaperID', database='MACG')
    cited_db1 = get_data_from_table_concurrent('paper_reference_no_duplicate', key='citedpaperID', database='openalex')
    df_paper_reference = pd.concat([citing_db, citing_db1, cited_db, cited_db1])
else:
    citing_db = get_data_from_table_concurrent('paper_reference', key='citingpaperID')
    cited_db = get_data_from_table_concurrent('paper_reference', key='citedpaperID')
    df_paper_reference = pd.concat([citing_db, cited_db])
print('paper_reference original', df_paper_reference.shape)
df_paper_reference=df_paper_reference.drop_duplicates()
print('paper_reference drop_duplicates', df_paper_reference.shape)
df_paper_reference.to_csv(f'out/{field}/csv/paper_reference.csv',index=False)

df_authors = get_data_from_table_concurrent('authors', key='authorID', data=authors)
df_authors.to_csv(f'out/{field}/csv/authors.csv',index=False)

#######################################################################
# update authors_field
# 计算并添加作者在领域内的论文及引用数量，更新作者的引用总数信息
# 通过计算每位作者的引用次数数据，注意hIndex在 extract_abstract.py 中计算
#######################################################################
print("## Step 1: Calculate Paper Count", datetime.now().strftime('%H:%M:%S'))
paper_count = df_paper_author.groupby('authorID')['paperID'].count().reset_index(name='PaperCount_field')

print("## Step 2: Calculate Total Citations", datetime.now().strftime('%H:%M:%S'))
# rename columns of df_papers {a:b}
df_papers.rename(columns={'citationCount':'CitationCount'}, inplace=True)
total_citations = df_paper_author.merge(df_papers, on='paperID')
total_citations = total_citations[total_citations['CitationCount'] >= 0]
total_citations = total_citations.groupby('authorID')['CitationCount'].sum().reset_index(name='CitationCount_field')

print("## Step 3: Merge Calculations with df_authors", datetime.now().strftime('%H:%M:%S'))
df_authors = df_authors.merge(paper_count, on='authorID', how='left')
df_authors = df_authors.merge(total_citations, on='authorID', how='left')

df_authors['PaperCount_field'] = df_authors['PaperCount_field'].fillna(0)
df_authors['CitationCount_field'] = df_authors['CitationCount_field'].fillna(0)
df_authors['hIndex_field'] = 0


df_authors.to_csv(f'out/{field}/csv/authors.csv',index=False)


####################################################################################
# to_sql
# 读取四个子表，并上传到mysql。创建表后添加领域子表的mysql索引（例如在scigene_field_field库）
####################################################################################
print('## uploading papers', datetime.now().strftime('%H:%M:%S'))
# df_papers = pd.read_csv(f'out/{field}/csv/papers.csv')
print(df_papers, df_papers.shape)
df_papers.to_sql('papers',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=100),\
    "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
        "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()})

print('## uploading paper_author', datetime.now().strftime('%H:%M:%S'))
# paper_author = pd.read_csv(f'out/{field}/csv/paper_author.csv')
print(df_paper_author, df_paper_author.shape)
df_paper_author.to_sql('paper_author',con=engine,if_exists='replace',index=False, dtype={"paperID": sqlalchemy.types.NVARCHAR(length=15),\
    "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()})

print('## uploading paper_reference', datetime.now().strftime('%H:%M:%S'))
# df_paper_reference = pd.read_csv(f'out/{field}/csv/paper_reference.csv')
print(df_paper_reference, df_paper_reference.shape)
df_paper_reference.to_sql('paper_reference',con=engine,if_exists='replace',index=False, dtype={"citingpaperID": sqlalchemy.types.NVARCHAR(length=15),\
    "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)})

print('## uploading authors', datetime.now().strftime('%H:%M:%S'))
# authors = pd.read_csv(f'out/{field}/csv/authors.csv')
print(df_authors, df_authors.shape)
df_authors.to_sql('authors',con=engine,if_exists='replace',index=False, dtype={"authorID": sqlalchemy.types.NVARCHAR(length=15),\
    "name": sqlalchemy.types.NVARCHAR(length=999),"rank":sqlalchemy.types.INTEGER(),"PaperCount":sqlalchemy.types.INTEGER(),"CitationCount":sqlalchemy.types.INTEGER()})

# add index
print('## add index', datetime.now().strftime('%H:%M:%S'))
execute('''ALTER TABLE papers ADD CONSTRAINT papers_pk PRIMARY KEY (paperID);
alter table papers add index(citationCount);
alter table paper_author add index(paperID);
alter table paper_author add index(authorID);
alter table paper_author add index(authorOrder);
alter table authors add index(authorID);
alter table authors add index(name);
alter table paper_reference add index(citingpaperID);
alter table paper_reference add index(citedpaperID);
ALTER TABLE paper_reference ADD CONSTRAINT paper_reference_pk PRIMARY KEY (citingpaperID,citedpaperID);
''')
       
# 直接update abstract太慢了，后续使用多进程下载
'''
alter table papers ADD abstract mediumtext;
update papers as P, {databaseMAG}.abstracts as abs set P.abstract = abs.abstract where P.paperID = abs.paperID

-- delete abstract mediumtext
ALTER TABLE papers DROP abstract;
'''
