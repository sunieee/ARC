import pandas as pd
import time
import pymysql
import os
from tqdm import tqdm
import json
import multiprocessing


field = os.environ.get('field')
process_num = multiprocessing.cpu_count()

def create_connection(database):
    conn = pymysql.connect(host='localhost',
                                user=os.environ.get('user'),
                                password=os.environ.get('password'),
                                db=database,
                                charset='utf8')
    return conn, conn.cursor()

edge_df = pd.read_csv(f'out/{field}/edge_proba.csv', dtype={'citingpaperID': str, 'citedpaperID': str})

def fetch_citation_context(pairs):
    conn, cursor = create_connection('MACG')
    dic = {}
    for pair in tqdm(pairs):
        citingpaperID, citedpaperID = pair
        query = f"""SELECT citation_context FROM MACG.CitationContextContent
                    where citingpaperID='{citingpaperID}'
                    and citedpaperID='{citedpaperID}'"""
        cursor.execute(query)
        result = cursor.fetchall()
        dic[citingpaperID + ',' + citedpaperID] = '\t'.join([row[0] for row in result])

    conn.close()
    return dic
        

if os.path.exists(f'out/{field}/citation_context.json'):
    print('loading citation context...')
    with open(f'out/{field}/citation_context.json', 'r') as f:
        citation2context = json.load(f)
else:
    print('extracting citation context...')
    t = time.time()
    data = list(zip(edge_df['citingpaperID'], edge_df['citedpaperID']))
    with multiprocessing.Pool(processes=process_num) as pool:
        results = pool.map(fetch_citation_context, [data[i::process_num] for i in range(process_num)])
    citation2context = {}
    for result in results:
        citation2context.update(result)
    print('time cost:', time.time()-t)
    with open(f'out/{field}/citation_context.json', 'w') as f:
        json.dump(citation2context, f)

edge_df = edge_df[['citingpaperID', 'citedpaperID', 'proba']]
edge_df.columns = ['childrenID', 'parentID', 'extendsProb']
edge_df['citationContext'] = edge_df.apply(lambda row: citation2context.get(row['childrenID'] + ',' + row['parentID']), axis=1)
edge_df.to_csv(f'out/{field}/links.csv', index=False)
