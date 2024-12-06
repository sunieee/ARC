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
import datetime
import json
from utils import field, cursor, field_info
from collections import defaultdict


userpass = f'{os.environ.get("user")}:{os.environ.get("password")}'
databaseMAG = os.environ.get('database', 'MACG')
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
    words = full_string.split()
    if len(words) == 1:
        return '', full_string
    first_word_parts = words[0].rsplit('_', 1)
    if len(first_word_parts) == 2:
        abbr = first_word_parts[0].replace('_', ' ')
        name = first_word_parts[1] + ' ' + ' '.join(words[1:])
        return abbr, name
    
    if '_' in full_string:
        return full_string.split('_', 1)
    return '', full_string


journal_conference = pd.DataFrame(columns=['type', 'original', 'ID', 'name'])

for journalID in field_info.get('journalID', []) + field_info.get('JournalID', []):
    print(journalID)
    sql_data = f'select name from journals where JournalID=\'{journalID}\';'
    journalName = pd.read_sql_query(sql_data, engine)['name'].tolist()[0]
    journal_conference.loc[len(journal_conference)] = {
        'type': 'journalID',
        'original': journalID,
        'ID': journalID,
        'name': journalName
    }

pattern = {
    ' and ': [' & '],
    '_': [':', ' -', '-', '–'],
    '/': [' '],
    ' on ': [' in ']
}
unmatchedJournal = []
for original in field_info.get('journal', []):
    journalName = original
    sql_data = f"select JournalID, name from journals where name='{journalName}';"
    print('*', sql_data)
    db_data = pd.read_sql_query(sql_data, engine)

    if len(db_data) == 0 :
        sql_data = f"select JournalID, name from journals where name like '%%{journalName}%%';"
        print('*', sql_data)
        db_data = pd.read_sql_query(sql_data, engine)

    if len(db_data) == 0 :
        for k, v in pattern.items():
            if original.count(k):
                for _v in v:
                    journalName = original.replace(k, _v)
                    sql_data = f"select JournalID, name from journals where name like '%%{journalName}%%';"
                    print('*', sql_data)
                    db_data = pd.read_sql_query(sql_data, engine)
                    if len(db_data) > 0:
                        break
    
    for row in db_data.to_records():
        journal_conference.loc[len(journal_conference)] = {
            'type': 'journal',
            'original': original,
            'ID': row['JournalID'],
            'name': row['name']
        }

    if len(db_data) == 0:
        print(f'{original} not found')
        unmatchedJournal.append(original)
        journal_conference.loc[len(journal_conference)] = {
            'type': 'journal',
            'original': original,
            'ID': None,
            'name': None
        }

# conferenceIDs = set(field_info.get('conferenceID', []))
for conferenceID in field_info.get('conferenceID', []) + field_info.get('ConferenceID', []):
    sql_data = f'select abbreviation, name from conferences where ConferenceID=\'{conferenceID}\';'
    abbreviation, conferenceName = pd.read_sql_query(sql_data, engine).values[0]
    journal_conference.loc[len(journal_conference)] = {
        'type': 'conferenceID',
        'original': conferenceID,
        'ID': conferenceID,
        'name': abbreviation + '_' + conferenceName
    }

for original in unmatchedJournal + field_info.get('conference', []):
    # abbreviation, conferenceName = original.split('_', 1)
    print(original)
    abbreviation, conferenceName = split_string(original)
    sql_data = f"select ConferenceID, abbreviation, name from conferences where name='{conferenceName}';"
    print('*', sql_data)
    db_data = pd.read_sql_query(sql_data, engine)

    if len(db_data) == 0:
        sql_data = f"select ConferenceID, abbreviation, name from conferences where name like '%%{conferenceName}%%';"
        print('*', sql_data)
        db_data = pd.read_sql_query(sql_data, engine)

    if len(db_data) == 0:
        sql_data = f"select ConferenceID, abbreviation, name from conferences where abbreviation='{abbreviation}';"
        print('*', sql_data)
        db_data = pd.read_sql_query(sql_data, engine)

    if len(db_data) == 0 :
        for k, v in pattern.items():
            if conferenceName.count(k):
                for _v in v:
                    t = conferenceName.replace(k, _v)
                    sql_data = f"select ConferenceID, abbreviation, name from conferences where name like '%%{t}%%';"
                    print('*', sql_data)
                    db_data = pd.read_sql_query(sql_data, engine)
                    if len(db_data) > 0:
                        break

    for row in db_data.to_records():
        journal_conference.loc[len(journal_conference)] = {
            'type': 'conference',
            'original': original,
            'ID': row['ConferenceID'],
            'name': row['abbreviation'] + '_' + row['name']
        }

    if len(db_data) == 0:
        print(f'{conferenceName} not found')
        journal_conference.loc[len(journal_conference)] = {
            'type': 'conference',
            'original': original,
            'ID': None,
            'name': None
        }

journal_conference.drop_duplicates(inplace=True)
journal_conference.to_csv(f'out/{field}/journal_conference.csv', index=False)
