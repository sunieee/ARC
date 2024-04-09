import pandas as pd
import pymysql
import datetime
import os
from collections import defaultdict
import math
import json
from tqdm import tqdm
import multiprocessing
from utils_scholar import *
import numpy as np

paperID2year = df_papers.set_index('paperID')['year'].to_dict()

authorID2yearCountMap = {}
coAuthorID2yearCountMap = {}
batch_size = 500

def getAuthorYearCountMap(authorID, conn):
    # 2024.3.9 注意这个函数有概率卡主，当paperID_list数量超过1000时，select ... from papers会卡主！必须分batch
    if authorID in authorID2yearCountMap:
        return authorID2yearCountMap[authorID]
    #
    yearCountMap = defaultdict(int)
    weightedYearCountMap = defaultdict(float)
    #
    if authorID not in authorID_list:
        df = pd.read_sql_query(f"select * from paper_author where authorID='{authorID}'", conn)   
        # renew paperID2year, add new paperID
        paperID_list = [x for x in df['paperID'].tolist() if x not in paperID2year]
        if len(paperID_list):
            for i in range(0, len(paperID_list), batch_size):
                paperID_str = ','.join([f'\'{x}\'' for x in paperID_list[i:i+batch_size]])
                papers = pd.read_sql_query(f"select paperID, PublicationDate from papers where paperID in ({paperID_str})", conn)
                papers['PublicationDate'] = pd.to_datetime(papers['PublicationDate'], errors='coerce')
                for paper_id, publication_date in zip(papers['paperID'], papers['PublicationDate']):
                    if paper_id not in paperID2year:
                        paperID2year[paper_id] = publication_date.year if pd.notna(publication_date) else 0
    else:
        df = df_paper_author[df_paper_author['authorID'] == authorID]
    #
    for index, row in df.iterrows():
        paperID = row['paperID']
        year = paperID2year[paperID]
        yearCountMap[year] += 1
        weightedYearCountMap[year] += 1 / row['authorOrder']
    authorID2yearCountMap[authorID] = yearCountMap, weightedYearCountMap
    return yearCountMap, weightedYearCountMap


def getCoAuthorYearCountMap(coAuthorID, conn):
    if coAuthorID in coAuthorID2yearCountMap:
        return coAuthorID2yearCountMap[coAuthorID]
    #
    yearCountMap = defaultdict(int)
    weightedYearCountMap = defaultdict(float)
    #
    studentID, supervisorID = coAuthorID.split('-')
    paperID_list = df_paper_author[df_paper_author['authorID'] == supervisorID]['paperID'].tolist()
    dfs = []
    for i in range(0, len(paperID_list), batch_size):
        paperID_str = ','.join([f'\'{x}\'' for x in paperID_list[i:i+batch_size]])
        df = pd.read_sql_query(f"select * from paper_author where authorID='{studentID}' and paperID in ({paperID_str})", conn)
        dfs.append(df)
    df = pd.concat(dfs)
    #
    for index, row in df.iterrows():
        paperID = row['paperID']
        year = paperID2year[paperID]
        yearCountMap[year] += 1
        weightedYearCountMap[year] += 1 / row['authorOrder']
    coAuthorID2yearCountMap[coAuthorID] = yearCountMap, weightedYearCountMap
    return yearCountMap, weightedYearCountMap



MIN_SUPERVISOR_RATE = 0.3
MIN_SUPERVISED_RATE = 1
MIN_SUPERVISING_RATE = 1
MIN_SUPERVISED_YEAR_SPAN = 2
MIN_SUPERVISED_PAPER_SPAN = 2.1
MAX_SUPERVISED_YEAR = 6
HALF_SUPERVISED_YEAR = 3
MAX_YEAR = 10000

MAX_SUPERVISED_PAPER = 10
HALF_SUPERVISED_PAPER = 5
MAX_PAPER = 10000

MAX_ACADEMIC_YEAR = int(
    MAX_SUPERVISED_YEAR
    - 1
    - math.log(MIN_SUPERVISOR_RATE * MIN_SUPERVISED_RATE)
    * HALF_SUPERVISED_YEAR
    / math.log(2)
)

SUPERVISED_YEAR_MODIFIER = []

for i in range(MAX_YEAR):
    if i < MAX_SUPERVISED_YEAR:
        SUPERVISED_YEAR_MODIFIER.append(1)
    else:
        SUPERVISED_YEAR_MODIFIER.append(
            math.exp(
                -math.log(2) * (i - MAX_SUPERVISED_YEAR + 1) / HALF_SUPERVISED_YEAR
            )
        )

SUPERVISED_PAPER_MODIFIER = []

for i in range(MAX_PAPER):
    if i < MAX_SUPERVISED_PAPER:
        SUPERVISED_PAPER_MODIFIER.append(1)
    else:
        SUPERVISED_PAPER_MODIFIER.append(
            math.exp(
                -math.log(2) * (i - MAX_SUPERVISED_PAPER + 1) / HALF_SUPERVISED_PAPER
            )
        )


def compute_count_list(academic_year_list, paper_count_map, start_list=None):
    count_list = [0]
    for i in range(1, len(academic_year_list)):
        assert len(count_list) == i
        count = paper_count_map.get(academic_year_list[i - 1], 0)
        if start_list:
            count *= min(SUPERVISED_YEAR_MODIFIER[i - 1], 
                         SUPERVISED_PAPER_MODIFIER[
                             int(start_list[i - 1])])
        count_list.append(count_list[-1] + count)
    return count_list


def compute_total_count(paper_count_map, year):
    academic_year_list = sorted(paper_count_map.keys())
    current_year_index = academic_year_list.index(year)
    total_count = 0
    for i in range(current_year_index):
        total_count += paper_count_map.get(academic_year_list[i], 0)
    return total_count


def compute_supervisor_rate(studentID, supervisorID, year, conn):
    paperCountMap, weightedPaperCountMap = getAuthorYearCountMap(studentID, conn)
    coAuthorID = f"{studentID}-{supervisorID}"
    coPaperCountMap, coWeightedPaperCountMap = getCoAuthorYearCountMap(coAuthorID, conn)
    #
    student_academic_years = sorted(list(paperCountMap.keys()))[: MAX_ACADEMIC_YEAR + 1]
    if not (year in student_academic_years):
        return 0.0
    yearIndex = student_academic_years.index(year)
    #
    start_student_count = compute_count_list(student_academic_years, weightedPaperCountMap)
    end_student_count = compute_count_list(student_academic_years[::-1], weightedPaperCountMap)[::-1]
    # the same as below except that co-author weighted count is replaced by student weighted count
    total_student_count = start_student_count[yearIndex] + end_student_count[yearIndex] + weightedPaperCountMap[year];
    start_coauthor_count = compute_count_list(student_academic_years, coWeightedPaperCountMap, start_student_count)
    end_coauthor_count = compute_count_list(student_academic_years[::-1], coWeightedPaperCountMap, start_student_count)[::-1]
    #
    total_coauthor_count = (
        start_coauthor_count[yearIndex] + end_coauthor_count[yearIndex]
        + coWeightedPaperCountMap[year]
        * min(SUPERVISED_YEAR_MODIFIER[yearIndex],
            SUPERVISED_PAPER_MODIFIER[int(start_student_count[yearIndex])],
        )
    )
    # iterate all possible year span (window) to compute the max supervisedRate
    maxSupervisedRate = 0.0
    maxStart, maxEnd = 0, 0
    for start_year_index in range(0, yearIndex + 1):
        for end_year_index in range(yearIndex, len(student_academic_years)):
            # there is a problem here: the co-authorship can happen in the same year,
            # because the surrounding years may not have co-authorship between student and supervisor
            # then the small window with year_span >= 2 can still be the maximal because the co-authorship
            # are too centralized in the same year
            #
            # we solve it by using a count list for co-authorship years
            #
            if (end_year_index - start_year_index + 1) < MIN_SUPERVISED_YEAR_SPAN:
                continue
            denominator =  total_student_count - start_student_count[start_year_index] - end_student_count[end_year_index]
            if denominator < MIN_SUPERVISED_PAPER_SPAN:
                continue
            numerator =  total_coauthor_count - start_coauthor_count[start_year_index] - end_coauthor_count[end_year_index]
            supervisedRate = numerator / denominator
            if supervisedRate > maxSupervisedRate:
                maxSupervisedRate = supervisedRate
                maxStart, maxEnd = start_year_index, end_year_index
    #
    maxSupervisedRate = min(1.0, maxSupervisedRate / MIN_SUPERVISED_RATE)
    # compute supervising rate
    total_supervisor_count = compute_total_count(getAuthorYearCountMap(supervisorID, conn)[0], year)
    total_coauthor_count = compute_total_count(coPaperCountMap, year)
    denominator = total_coauthor_count
    numerator = total_supervisor_count - total_coauthor_count
    #
    if numerator < 0:
        print(f"Error in computation, supervisor paper count smaller than co-author paper count: {studentID}, {supervisorID}")
        supervisingRate = 0.0
    elif numerator == 0:
        supervisingRate = 0.0
    elif denominator == 0:
        supervisingRate = MIN_SUPERVISING_RATE
    else:
        supervisingRate = numerator / denominator
    #
    supervisingRate = min(1.0, supervisingRate / MIN_SUPERVISING_RATE)
    # print(paperID, student_academic_years, maxSupervisedRate * supervisingRate)
    return maxSupervisedRate * supervisingRate


print('# start to process each author (key paper)', len(authorID_list), datetime.now().strftime("%H:%M:%S"))
multiprocess_num = multiprocessing.cpu_count()

def toStr(s):
    if type(s) == str:
        return s
    return s.iloc[0]


def build_top_author(authorID):
    if os.path.exists(f'out/{field}/papers_raw/{authorID}.csv'):
        return
    print('## ' + authorID)
    conn, cursor = create_connection()
    # Filter out rows from df_paper_author for the specific authorID
    df_paper_author_author = df_paper_author[df_paper_author['authorID'] == authorID]
    # print('df_paper_author_author', len(df_paper_author_author))
    df = df_papers.merge(df_paper_author_author, on="paperID").groupby(['paperID', 'title', 'year']).agg({
        'authorOrder': 'min'
    }).reset_index()
    print('df', len(df))
    df['isKeyPaper'] = 0.0
    # Get the first author ID for each paper
    paperID_list = df['paperID'].tolist()
    if paperID_list:
        paperID_str = ','.join([f'\'{x}\'' for x in paperID_list])
        # print('Get first author ID', len(paperID_list))
        cursor.execute(f"select paperID, authorID from paper_author where authorOrder = 1 and paperID in ({paperID_str})")
        paperID2FirstAuthorID = dict(cursor.fetchall())
        df['firstAuthorID'] = df['paperID'].map(paperID2FirstAuthorID)
    else:
        df['firstAuthorID'] = np.nan

    # Process the DataFrame
    for i, row in df.iterrows():
        print('### ' + row['paperID'])
        if pd.isna(row['firstAuthorID']):
            authorOrder = int(row['authorOrder'])
            print('Target paper does not have first author!', row['paperID'], authorOrder)
            isKeyPaper = 1 / authorOrder
        else:
            if row['firstAuthorID'] == authorID:
                isKeyPaper = 1
            else:
                # print('Compute supervisor rate', toStr(row['firstAuthorID']), authorID, int(row['year']))
                isKeyPaper = compute_supervisor_rate(toStr(row['firstAuthorID']), authorID, int(row['year']), conn)
        df.at[i, 'isKeyPaper'] = isKeyPaper
        print(row['paperID'], isKeyPaper)

    df.to_csv(f'out/{field}/papers_raw/{authorID}.csv', index=False)
    cursor.close()
    conn.close()


# os.makedirs(f'out/{field}/papers_raw', exist_ok=True)
# with multiprocessing.Pool(processes=multiprocess_num) as pool:
#     pool.map(build_top_author, authorID_list)

for authorID in authorID_list:
    build_top_author(authorID)


with open(f'{path}/authorID2yearCountMap.json', 'w') as f:
    json.dump(authorID2yearCountMap, f)
with open(f'{path}/coAuthorID2yearCountMap.json', 'w') as f:
    json.dump(coAuthorID2yearCountMap, f)