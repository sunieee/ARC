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

userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
upload=True

field = os.environ.get('field')
folders = [x for x in os.listdir('out') if x.startswith(field) and x != field]
# folders = ['AI1', 'AI2', 'AI3', 'AI4']
print('source folders:', folders)
print('target folder:', field)

dtype_dic = {
    'papers': {"paperID": sqlalchemy.types.NVARCHAR(length=100), "title": sqlalchemy.types.NVARCHAR(length=2000),"ConferenceID": sqlalchemy.types.NVARCHAR(length=15),"JournalID": sqlalchemy.types.NVARCHAR(length=15),\
            "rank":sqlalchemy.types.INTEGER(),"referenceCount":sqlalchemy.types.INTEGER(),"citationCount":sqlalchemy.types.INTEGER(),"PublicationDate":sqlalchemy.types.Date()},
    'paper_author': {"paperID": sqlalchemy.types.NVARCHAR(length=15), "authorID": sqlalchemy.types.NVARCHAR(length=15),"authorOrder":sqlalchemy.types.INTEGER()},
    'paper_reference': {"citingpaperID": sqlalchemy.types.NVARCHAR(length=15), "citedpaperID": sqlalchemy.types.NVARCHAR(length=15)},
    'authors': {"authorID": sqlalchemy.types.NVARCHAR(length=15), "name": sqlalchemy.types.NVARCHAR(length=999),"rank":sqlalchemy.types.INTEGER(),"PaperCount":sqlalchemy.types.INTEGER(),"CitationCount":sqlalchemy.types.INTEGER()}
}

def merge_database(name):
    df = []
    if os.path.exists(f'out/{field}/csv/{name}.csv'):
        print(f'## reading {name}', datetime.now().strftime('%H:%M:%S'))
        df = pd.read_csv(f'out/{field}/csv/{name}.csv')
    else:
        print(f'## merging {name}', datetime.now().strftime('%H:%M:%S'))
        for folder in tqdm(folders):
            t = pd.read_csv(f'out/{folder}/csv/{name}.csv')
            # 删除所有 'Unnamed' 开头的列
            t = t.loc[:, ~t.columns.str.contains('^Unnamed')]
            df.append(t)
        
        df = pd.concat(df)
        print('dropping duplicates', df.head(), df.shape, datetime.now().strftime('%H:%M:%S'))
        df.drop_duplicates(inplace=True)
        df.to_csv(f'out/{field}/csv/{name}.csv', index=False)

    if upload:
        print('uploading to sql', df.head(), df.shape, datetime.now().strftime('%H:%M:%S'))
        df.to_sql(
            name=name, 
            con=engine, 
            if_exists='replace', 
            dtype=dtype_dic[name], 
            chunksize=1000  # 每次插入1000行
        )
    return df

df_papers = merge_database('papers')
df_paper_author = merge_database('paper_author')
df_authors = merge_database('authors')

# df_papers = pd.read_csv(f'out/{field}/csv/papers.csv')
# df_paper_author = pd.read_csv(f'out/{field}/csv/paper_author.csv')
# df_authors = pd.read_csv(f'out/{field}/csv/authors.csv')

df_authors = df_authors[df_authors.columns.drop(list(df_authors.filter(regex='^PaperCount_field')))]
df_authors = df_authors[df_authors.columns.drop(list(df_authors.filter(regex='^CitationCount_field')))]

df_papers['paperID'] = df_papers['paperID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_authors['authorID'] = df_authors['authorID'].astype(str)


#######################################################################
# update authors_field
# 计算并添加作者在领域内的论文及引用数量，更新作者的引用总数信息
# 通过计算每位作者的引用次数数据，根据 h-index 的定义，计算并更新了每位作者在特定领域内的 h-index 值
#######################################################################
print("## Step 1: Calculate Paper Count", datetime.now().strftime('%H:%M:%S'))
paper_count = df_paper_author.groupby('authorID')['paperID'].count().reset_index(name='PaperCount_field')
print('paper_count:', paper_count.head(), paper_count.shape)

print("## Step 2: Calculate Total Citations", datetime.now().strftime('%H:%M:%S'))
df_papers.rename(columns={'citationCount':'CitationCount'}, inplace=True)
df_papers = df_papers[['paperID', 'CitationCount']]
df_papers = df_papers[df_papers['CitationCount'] >= 0]
total_citations = df_paper_author.merge(df_papers, on='paperID')
total_citations = total_citations.groupby('authorID')['CitationCount'].sum().reset_index(name='CitationCount_field')
print('total_citations:', total_citations.head(), total_citations.shape)

print("## Step 3: Merge Calculations with df_authors", datetime.now().strftime('%H:%M:%S'))
df_authors = df_authors.merge(paper_count, on='authorID', how='left')
df_authors = df_authors.merge(total_citations, on='authorID', how='left')

df_authors.to_csv(f'out/{field}/csv/authors.csv',index=False)

df_authors['PaperCount_field'] = df_authors['PaperCount_field'].fillna(0)
df_authors['CitationCount_field'] = df_authors['CitationCount_field'].fillna(0)
df_authors['hIndex_field'] = 0

df_authors.to_csv(f'out/{field}/csv/authors.csv',index=False)
if upload:
    df_authors.to_sql('authors_field',con=engine,if_exists='replace',index=False, dtype=dtype_dic['authors'], chunksize=1000)
    
    # add index
    print('## add index', datetime.now().strftime('%H:%M:%S'))
    execute('''ALTER TABLE papers_field ADD CONSTRAINT papers_field_pk PRIMARY KEY (paperID);
    alter table papers_field add index(citationCount);
    alter table paper_author_field add index(paperID);
    alter table paper_author_field add index(authorID);
    alter table paper_author_field add index(authorOrder);
    alter table authors_field add index(authorID);
    alter table authors_field add index(name);
    ''')
        

merge_database('paper_reference')

if upload:
    execute('''alter table paper_reference_field add index(citingpaperID);
    alter table paper_reference_field add index(citedpaperID);
    ALTER TABLE paper_reference_field ADD CONSTRAINT paper_reference_field_pk PRIMARY KEY (citingpaperID,citedpaperID);
    ''')