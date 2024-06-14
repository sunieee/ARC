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

# 第一步：检索相关领域
query_1 = """
SELECT fieldID 
FROM MACG.field_of_study 
WHERE name LIKE '%graph vis%' 
OR name LIKE '%network vis%';
"""
cursor.execute(query_1)
field_ids = cursor.fetchall()
field_ids = [row[0] for row in field_ids]

# 第二步：根据领域ID检索相关文章
placeholders = ','.join(['%s'] * len(field_ids))
query_2 = f"""
SELECT B.* 
FROM papers_field AS A 
JOIN papers AS B 
ON A.paperID = B.paperID 
WHERE A.fieldID IN ({placeholders});
"""
cursor.execute(query_2, field_ids)
papers_from_fields = cursor.fetchall()
papers_columns = [desc[0] for desc in cursor.description]
papers_from_fields_df = pd.DataFrame(papers_from_fields, columns=papers_columns)

# 第三步：检索visualization领域中与话题相关的文章
query_3 = """
SELECT * 
FROM scigene_visualization_field.papers_field 
WHERE title LIKE '%graph vis%' 
OR title LIKE '%network vis%';
"""
cursor.execute(query_3)
visualization_related_papers = cursor.fetchall()
visualization_columns = [desc[0] for desc in cursor.description]
visualization_related_papers_df = pd.DataFrame(visualization_related_papers, columns=visualization_columns)

# 合并结果
papers = pd.concat([papers_from_fields_df, visualization_related_papers_df]).drop_duplicates()

papers.drop(columns=['abstract', 'citationCountByYear'], inplace=True)
papers['PublicationDate'] = pd.to_datetime(papers['PublicationDate'])
papers.to_csv(f'out/{field}/papers.csv')