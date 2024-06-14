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
df_papers = pd.read_csv(f'out/{field}/papers.csv', dtype={'paperID': str})

print(f'loading data from database: {len(df_papers)} papers', datetime.now().strftime("%H:%M:%S"))
batch_size = 500
path = f'out/{field}'

def extract_batch(batches):
    conn, _ = create_connection()
    paper_reference_batches = []
    paper_author_batches = []
    for batch in tqdm(batches):
        paperID_str = ','.join([f'\'{x}\'' for x in batch])

        paper_author_batches.append(
            pd.read_sql_query(f"SELECT * FROM paper_author WHERE paperID IN ({paperID_str})", conn))
        paper_reference_batches.append(
            pd.read_sql_query(f"SELECT * FROM paper_reference WHERE citingpaperID IN ({paperID_str})", conn))
        paper_reference_batches.append(
            pd.read_sql_query(f"SELECT * FROM paper_reference WHERE citedpaperID IN ({paperID_str})", conn))

    return pd.concat(paper_author_batches, ignore_index=True), pd.concat(paper_reference_batches, ignore_index=True)


if not os.path.exists(f'{path}/paper_reference.csv'):
    os.makedirs(path, exist_ok=True)
    paperID_list = set(df_papers['paperID'].tolist())
    
    # 将paperID_list分成多个批次,定义每个批次的大小
    paperID_batches = [list(paperID_list)[i:i + batch_size] for i in range(0, len(paperID_list), batch_size)]
    print('paperID_batches:', len(paperID_batches))

    df_paper_author, df_paper_reference = extract_batch(paperID_batches)
    df_paper_author.to_csv(f"{path}/paper_author.csv", index=False)
    df_paper_reference.to_csv(f"{path}/paper_reference.csv", index=False)
    df_paper_reference = None
else:
    df_papers = pd.read_csv(f"{path}/papers.csv")
    df_paper_author = pd.read_csv(f"{path}/paper_author.csv")
    
df_paper_author['authorID'] = df_paper_author['authorID'].astype(str)
df_paper_author['paperID'] = df_paper_author['paperID'].astype(str)
df_papers['paperID'] = df_papers['paperID'].astype(str)

