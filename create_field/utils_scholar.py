import pandas as pd
import pymysql
from datetime import datetime
import os
from tqdm import tqdm
import multiprocessing


def create_connection(database='MACG'):
    conn = pymysql.connect(host='localhost',
                            user='root',
                            password='root',
                            db=database,
                            charset='utf8')
    return conn, conn.cursor()

field = os.environ.get('field')
database = os.environ.get('database', 'MACG')

conn, cursor = create_connection()
df_authors = pd.read_csv(f'out/{field}/authors.csv', dtype={'authorID': str})

authorIDs = set(df_authors['authorID'].tolist())
authorID_list = list(authorIDs)
authorID_str = ','.join([f'\'{x}\'' for x in authorID_list])

print(f'loading data from database: {len(authorID_list)} authors', datetime.now().strftime("%H:%M:%S"))
batch_size = 500
path = f'out/{field}/csv'


def extract_batch(batches):
    conn, _ = create_connection()
    df_papers_batch = []
    paper_reference_batches = []
    for batch in tqdm(batches):
        paperID_str = ','.join([f'\'{x}\'' for x in batch])

        # 查询papers表
        query_papers = f"SELECT * FROM papers WHERE paperID IN ({paperID_str})"
        df_papers_batch = pd.read_sql_query(query_papers, conn)
        papers_batches.append(df_papers_batch)

        paper_reference_batches.append(
            pd.read_sql_query(f"SELECT * FROM paper_reference WHERE citingpaperID IN ({paperID_str})", conn))
        paper_reference_batches.append(
            pd.read_sql_query(f"SELECT * FROM paper_reference WHERE citedpaperID IN ({paperID_str})", conn))
        
    df_papers = pd.concat(papers_batches, ignore_index=True)
    df_paper_reference = pd.concat(paper_reference_batches, ignore_index=True)
    return df_papers, df_paper_reference
            



if not os.path.exists(f'{path}/paper_reference.csv'):
    os.makedirs(path, exist_ok=True)
    if database == 'MACG':
        if os.path.exists(f"{path}/paper_author.csv"):
            df_paper_author = pd.read_csv(f"{path}/paper_author.csv", dtype={'authorID': str, 'paperID': str})
        else:
            dfs = []
            for i in range(0, len(authorID_list), batch_size):
                authorID_str = ','.join([f'\'{x}\'' for x in authorID_list[i:i + batch_size]])
                df_paper_author = pd.read_sql_query(f"select * from paper_author where authorID in ({authorID_str})", conn)
                dfs.append(df_paper_author)
            df_paper_author = pd.concat(dfs, ignore_index=True)
            df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)

        paperID_list = set(df_paper_author['paperID'].tolist())
        
        # 将paperID_list分成多个批次,定义每个批次的大小
        paperID_batches = [list(paperID_list)[i:i + batch_size] for i in range(0, len(paperID_list), batch_size)]
        print('paperID_batches:', len(paperID_batches))

        # 初始化空列表来存储每个批次的查询结果
        papers_batches = []
        paper_reference_batches = []

        # 对每个批次执行查询
        df_papers, df_paper_reference = extract_batch(paperID_batches)

        """
        # 使用多进程，为什么使用多进程后卡主不动？？？
        multiprocess_num = multiprocessing.cpu_count()
        paperID_batches_list = [list(paperID_list)[i::multiprocess_num] for i in range(multiprocess_num)]
        print('paperID_batches_list:', len(paperID_batches_list))
        with multiprocessing.Pool(multiprocess_num) as pool:
            results = pool.map(extract_batch, paperID_batches_list)
        for result in results:
            papers_batches.append(result[0])
            paper_reference_batches.append(result[1])

        df_papers = pd.concat(papers_batches, ignore_index=True)
        df_paper_reference = pd.concat(paper_reference_batches, ignore_index=True)
        """
        
        df_papers.to_csv(f"{path}/papers.csv", index=False)
        df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
        df_paper_reference = papers_batches = paper_reference_batches = None
    else:
        df_paper_author = pd.read_csv(f"out/{database}/csv/paper_author.csv", dtype={'authorID': str, 'paperID': str})
        df_paper_author = df_paper_author[df_paper_author['authorID'].isin(authorID_list)]
        df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
        paperIDs = set(df_paper_author['paperID'].tolist())

        df_papers = pd.read_csv(f"out/{database}/csv/papers.csv")
        df_papers = df_papers[df_papers['paperID'].isin(paperIDs)]
        df_papers.to_csv(f"{path}/papers.csv", index=False)

        df_paper_reference = pd.read_csv(f"out/{database}/csv/paper_reference.csv")
        df_paper_reference = df_paper_reference[df_paper_reference['citingpaperID'].isin(paperIDs) | df_paper_reference['citedpaperID'].isin(paperIDs)]
        df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
        df_paper_reference = None
else:
    df_paper_author = pd.read_csv(f"{path}/paper_author.csv")
    df_papers = pd.read_csv(f"{path}/papers.csv")
    
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
df_papers['paperID'] = df_papers['paperID'].astype(str)

"""
# 以下代码用于融合相同original（原始姓名）的人，仅在turing中使用过，在fellows中不需要进行合并
if os.environ.get('scholar') == '1' and not os.path.exists(f'out/{field}/authors.csv'):    
    typ = 10    # turing
    award_df = pd.DataFrame(columns=['original_author_name', 'year', 'type', 'MAGID', 'ARCID'])
    for row in df_authors.iterrows():
        row = row[1]
        award_df.loc[len(award_df)] =[row['name'], row['year'], typ, row['authorID'], 'NULL']
    award_df.to_csv(f'out/{field}/award_authors{typ}.csv', index=False)

    for original in tqdm(set(df_authors['original'].to_list())):
        df_authors_original = df_authors[df_authors['original'] == original].copy()
        if len(df_authors_original) == 1:
            continue
        print("merge authors:", original, len(df_authors_original))
        firstAuthorID = df_authors_original.iloc[0]['authorID']
        firstPaperCount = df_authors_original.iloc[0]['PaperCount']
        firstCitationCount = df_authors_original.iloc[0]['CitationCount']
        for row in df_authors_original.to_dict('records')[1:]:
            authorID = row['authorID']
            df_paper_author.loc[df_paper_author['authorID'] == authorID, 'authorID'] = firstAuthorID
            firstCitationCount += row['CitationCount']
            firstPaperCount += row['PaperCount']
        df_authors.loc[df_authors['original'] == original, 'PaperCount'] = firstPaperCount
        df_authors.loc[df_authors['original'] == original, 'CitationCount'] = firstCitationCount
    
    df_authors.drop_duplicates(subset=['original'], inplace=True, keep='first')
    df_authors.to_csv(f'out/{field}/authors.csv',index=False)
    df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
"""


if 'PaperCount_field' not in df_authors.columns:
    paper_count = df_paper_author.groupby('authorID')['paperID'].count().reset_index(name='PaperCount_field')
    df_authors = df_authors.merge(paper_count, on='authorID', how='left')
    df_authors['PaperCount_field'] = df_authors['PaperCount_field'].fillna(0)
    df_authors.to_csv(f'out/{field}/authors.csv',index=False)

df_papers['PublicationDate'] = pd.to_datetime(df_papers['PublicationDate'])
df_papers['year'] = df_papers['PublicationDate'].dt.year

