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
from datetime import datetime
import json
from utils import field, cursor, field_info
from collections import defaultdict


databaseMAG = os.environ.get('database', 'MACG')
userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
print(f'[extract_scigene_field] databaseMAG: {databaseMAG}, userpass: {userpass}')
engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/{databaseMAG}', pool_size=20)
GROUP_SIZE = 2000
multiproces_num = 20
paper_ids = set()

###########################################################################
# 根据名称匹配期刊和会议

paper_path = f'out/{field}/papers.txt'
if os.path.exists(paper_path):
    # if field == 'AI3':
    #     # paper_ids = set()
    #     paper_ids = set(np.loadtxt(paper_path, dtype=str))
    # else:
    raise "paper already exist!"

def split_string(full_string):
    # Split the string into words
    print('original string', full_string)
    words = full_string.split()
    if len(words) == 1:
        return '', full_string
    first_word_parts = words[0].rsplit('_', 1)
    if len(first_word_parts) == 2:
        abbr = first_word_parts[0].replace('_', ' ')
        name = first_word_parts[1] + ' ' + ' '.join(words[1:])
        return abbr, name
    return full_string.split('_', 1)


journal_conference = pd.read_csv(f'out/{field}/journal_conference_modify.csv')
id2row = {row['ID']: row for row in journal_conference.to_records()}

####################################################################################
# extract paperID
# 根据领域名称查询fieldID，如： select * from field_of_study where name='field';
####################################################################################

def get_paperID_batch(pair):
    fieldID, offset, ix, pbar, verbose = pair
    engine = create_engine(f'mysql+pymysql://{userpass}@192.168.0.140:3306/{databaseMAG}')
    sql_data = f'select paperID from papers_field where fieldID=\'{fieldID}\' LIMIT {GROUP_SIZE} OFFSET {offset};'
    # if verbose:
    #     time = datetime.now().strftime('%H:%M:%S')
    #     print(f'* {time} ' + sql_data)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    engine.dispose()
    if verbose:
        pbar.n = int(ix)
        pbar.refresh()
    return db_data


def get_field(field):
    pattern = {
        '-': ['–', ':', ' -'],
    }
    try:
        fieldID = int(field)
        cursor.execute(f"SELECT name, paperCount FROM {databaseMAG}.field_of_study where fieldID='{fieldID}'")
        result = cursor.fetchone()
        fieldName = result[0]
        paperCount = result[1]
    except:
        fieldName = field
        cursor.execute(f"SELECT fieldID, paperCount  FROM {databaseMAG}.field_of_study where name='{fieldName}'")
        result = cursor.fetchone()
        if result is not None:
            fieldID = result[0]
            paperCount = result[1]
        else:
            for k, v in pattern.items():
                if k in field:
                    for _v in v:
                        fieldName = field.replace(k, _v)
                        cursor.execute(f"SELECT fieldID, paperCount  FROM {databaseMAG}.field_of_study where name='{fieldName}'")
                        result = cursor.fetchone()
                        if result is not None:
                            fieldID = result[0]
                            paperCount = result[1]
                            break
        
    return fieldID, fieldName, paperCount


paper_count = pd.DataFrame(columns=['type', 'original', 'ID', 'name', '#paper', '#accumulate', '#addition', 'validRatio'])
def read_papers(fields, parent=None):
    verbose = parent is None
    for field in tqdm(fields):
        start_count = len(paper_ids)
        fieldID, fieldName, paperCount = get_field(field)
        print(f'read papers from field: {field}, {fieldID}, {fieldName}, {paperCount}')
        if paperCount < 10000:
            db_data = pd.read_sql_query(f'select paperID from papers_field where fieldID=\'{fieldID}\' and score>=0.2;', engine)['paperID'].tolist()
        else:
            group_num = paperCount // GROUP_SIZE + 1
            pbar = tqdm(total=group_num)
            print(f'filedID: {fieldID}, paperCount: {paperCount}, group_num: {group_num}')
            with concurrent.futures.ThreadPoolExecutor(max_workers=multiproces_num) as executor:
                results = executor.map(get_paperID_batch, [(fieldID, i*GROUP_SIZE, i, pbar, verbose) for i in range(group_num)])
            db_data = set()
            for result in results:
                db_data.update(result)

        paper_ids.update(db_data)
        paper_count.loc[len(paper_count)] = {
            'type': 'field' if verbose else 'children',
            'original': field if verbose else parent,
            'ID': fieldID,
            'name': fieldName,
            '#paper': len(db_data),
            '#accumulate': len(paper_ids),
            '#addition': len(paper_ids) - start_count,
            'validRatio': (len(paper_ids) - start_count) / len(db_data) if len(db_data) else 0
        }
        if verbose:
            print(f'finish reading paperID on {fieldName}({fieldID}), #paper: {len(db_data)}, #accumulate: {len(paper_ids)}')


read_papers(field_info.get('fieldID',[]))
read_papers(field_info.get('field', []))
print(f'## finish reading paperID on field:', len(paper_ids))


for f in field_info.get('children', []):
    fieldID, fieldName, paperCount = get_field(f)
    sql_data = f'select childrenID FROM field_children where parentID=\'{fieldID}\';'
    children_fields = pd.read_sql_query(sql_data, engine).values.ravel().tolist()
    print('*', sql_data, len(children_fields))
    read_papers(children_fields, parent=fieldName)
    print(f'finish reading paperID on children of {fieldName}({fieldID}), #accumulate: {len(paper_ids)}')

    print(f'### finish reading child: {f}, saving to txt', len(paper_ids))
    np.savetxt(paper_path, list(paper_ids), fmt='%s')
    paper_count.to_csv(f'out/{field}/paper_count.csv', index=False)

print(f'## finish reading paperID on children:', len(paper_ids))


journalIDs = set(journal_conference[journal_conference['type'].isin(['journalID', 'journal'])]['ID'].tolist())
journalIDs = [j for j in journalIDs if str(j) != 'nan' and str(j) != 'None']
for journalID in tqdm(journalIDs):
    sql_data = f'select paperID from papers where JournalID=\'{journalID}\';'
    print('*', sql_data)
    start_count = len(paper_ids)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    paper_ids.update(db_data)
    paper_count.loc[len(paper_count)] = {
        'type': id2row[journalID]['type'],
        'original': id2row[journalID]['original'], 
        'ID': journalID,
        'name': id2row[journalID]['name'],
        '#paper': len(db_data),
        '#accumulate': len(paper_ids),
        '#addition': len(paper_ids) - start_count,
        'validRatio': (len(paper_ids) - start_count) / len(db_data) if len(db_data) > 0 else 0
    }
    print(f'finish reading paperID on Journal {journalID}, single: {len(db_data)}, all: {len(paper_ids)}')
print(f'## finish reading paperID on Journal:', len(paper_ids))


conferenceIDs = set(journal_conference[journal_conference['type'].isin(['conferenceID', 'conference'])]['ID'].tolist())
# remove None and nan
conferenceIDs = [c for c in conferenceIDs if str(c) != 'nan' and str(c) != 'None']
for conferenceID in tqdm(conferenceIDs):
    sql_data = f'select paperID from papers where ConferenceID=\'{conferenceID}\';'
    print('*', sql_data)
    start_count = len(paper_ids)
    db_data = pd.read_sql_query(sql_data, engine)['paperID'].tolist()
    paper_ids.update(db_data)
    paper_count.loc[len(paper_count)] = {
        'type': id2row[conferenceID]['type'],
        'original': id2row[conferenceID]['original'],
        'ID': conferenceID,
        'name': id2row[conferenceID]['name'],
        '#paper': len(db_data),
        '#accumulate': len(paper_ids),
        '#addition': len(paper_ids) - start_count,
        'validRatio': (len(paper_ids) - start_count) / len(db_data) if len(db_data) > 0 else 0
    }
    print(f'finish reading paperID on Conference {conferenceID}, single: {len(db_data)}, all: {len(paper_ids)}')
print(f'## finish reading paperID on Conference:', len(paper_ids))

print('# finish reading MAG list from sql, saving to txt', len(paper_ids))
np.savetxt(paper_path, list(paper_ids), fmt='%s')

paper_count.to_csv(f'out/{field}/paper_count.csv', index=False)