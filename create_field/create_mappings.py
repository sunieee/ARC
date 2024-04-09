

from utils import *

df_papers, df_authors, df_paper_author, df_paper_author_filtered, top_authors = create_top()
path_to_mapping = f"out/{field}/csv"
authorID_list = top_authors['authorID'].tolist()

MIN_STUDENT_AUTHOR_ORDER = 3
print("Pre-compute first author maps!", datetime.now().strftime("%H:%M:%S"))
firstAuthorTmp = df_paper_author_filtered.merge(df_paper_author, on="paperID", suffixes=('', '_first')) \
    .query("authorOrder > 1 and authorOrder_first == 1") 
firstAuthorTmp = firstAuthorTmp[['authorID', 'paperID', 'authorOrder', 'authorID_first']]
firstAuthorTmp = firstAuthorTmp.groupby(['authorID', 'authorID_first']).size().reset_index(name='count')
firstAuthors = firstAuthorTmp['authorID_first'].unique().tolist()

'''
        authorID authorID_first  count
0      100461504     2012192335      2
1      100461504     2059806765      1
2      100461504     2263221087      3
3      100461504     2269301939      2
4      100461504     2277984915      1
...          ...            ...    ...
95058   99770489     2625989380      2
95059   99770489     2764676664      2
95060   99770489     2895040106      1
95061   99770489     3109868778      1
95062   99770489      837107254      3

[95063 rows x 3 columns]
'''

######################################################################
# 从数据库中查询领域的**firstAuthor + topAuthor **信息，构建两个映射：
# 1. paperCountMap：映射每个作者ID到一个子映射，其中子映射的键是年份，值是该年份的论文数量。
# 2. weightedPaperCountMap：与上面的映射类似，但值是加权的论文数量，
#       其中权重是1/作者顺序，但仅考虑作者顺序小于或等于MIN_STUDENT_AUTHOR_ORDER的作者。
######################################################################
print("compute first-author maps!", datetime.now().strftime("%H:%M:%S"))
merged_df = df_paper_author.merge(df_papers, on='paperID')
filtered_df = merged_df[merged_df['authorID'].isin(firstAuthors + authorID_list)]
grouped = filtered_df.groupby(['authorID', 'authorOrder', 'year']).size().reset_index(name='cnt')
paperCountMap = {}
weightedPaperCountMap = {}

for _, row in grouped.iterrows():
    authorID = row['authorID'].strip()
    authorOrder = int(row['authorOrder'])
    year = int(row['year'])
    count = int(row['cnt'])
    # 更新paperCountMap
    yearCountMap = paperCountMap.setdefault(authorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count
    # 更新weightedPaperCountMap
    if authorOrder <= MIN_STUDENT_AUTHOR_ORDER:
        yearWeightedCountMap = weightedPaperCountMap.setdefault(authorID, {})
        yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder


######################################################################
# 从数据库中查询领域的论文合作者信息，**相关作者（topAuthor）**有关的不是第一作者论文，并基于查询结果构建两个映射：
# 1. coWeightedPaperCountMap：映射每个合作者ID对（由两个作者ID组成）到一个子映射，
#   其中子映射的键是年份，值是加权的论文数量，其中权重是1/作者顺序。
# 2. coPaperCountMap：与上面的映射类似，但值是该年份的论文数量。
######################################################################
print("compute co-author maps!", datetime.now().strftime("%H:%M:%S"))
coauthor_joined = firstAuthorTmp.merge(
    df_paper_author[df_paper_author['authorOrder'] <= MIN_STUDENT_AUTHOR_ORDER], 
    left_on='authorID_first', right_on='authorID', suffixes=('', '_PA1')
).merge(
    df_paper_author, left_on=['authorID', 'paperID'], right_on=['authorID', 'paperID'], suffixes=('', '_PA2')
)
coauthor_joined = coauthor_joined[coauthor_joined['authorOrder'] < coauthor_joined['authorOrder_PA2']]
'''
PA1 是第一作者，PA2 是合作者，查询两者之间有没有其他的联合论文（也需要PA1.order < PA2.order）
                                                        PA1             PA1             PA2
        authorID authorID_first  count     paperID authorID_PA1  authorOrder  authorOrder_PA2
0        100461504     2012192335      2  1510416401   2012192335            1                2
1        100461504     2012192335      2  1486682013   2012192335            1                3
2       2429897830     2012192335      2  1490177788   2012192335            1                2
3       2429897830     2012192335      2  2100876320   2012192335            2                5
4       2429897830     2059806765      1  2100876320   2059806765            1                5
...            ...            ...    ...         ...          ...          ...              ...
240906    99467868     2020717950      2  2498751001   2020717950            1                2
240907    99467868       51191428      1  1438141466     51191428            1                4
240908    99770489     2124762050      1  2036318837   2124762050            1                2
240909    99770489     2327525373      1  2619713135   2327525373            1               10
240910    99770489     2895040106      1  2018431971   2895040106            1                2

[209923 rows x 7 columns]
'''
coauthor_year_joined = coauthor_joined.merge(df_papers[['paperID', 'year']], left_on="paperID", right_on="paperID", suffixes=('', '_P'))
coauthor_year_joined = coauthor_year_joined[['authorID_first', 'authorID', 'paperID', 'authorOrder', 'year']].drop_duplicates()
grouped = coauthor_year_joined.groupby(['authorID_first', 'authorID', 'authorOrder', 'year']).size().reset_index(name='count')
grouped['coAuthorID'] = grouped['authorID_first'] + "-" + grouped['authorID']

coWeightedPaperCountMap = {}
coPaperCountMap = {}

for _, row in grouped.iterrows():
    coAuthorID = row['coAuthorID']
    year = int(row['year'])
    count = row['count']
    authorOrder = row['authorOrder']
    # For coWeightedPaperCountMap
    yearWeightedCountMap = coWeightedPaperCountMap.setdefault(coAuthorID, {})
    yearWeightedCountMap[year] = yearWeightedCountMap.get(year, 0) + count / authorOrder
    # For coPaperCountMap
    yearCountMap = coPaperCountMap.setdefault(coAuthorID, {})
    yearCountMap[year] = yearCountMap.get(year, 0) + count


# save all the maps to {path_to_csv}/*.json
with open(f"{path_to_mapping}/paperCountMap.json", "w") as f:
    json.dump(paperCountMap, f)
with open(f"{path_to_mapping}/weightedPaperCountMap.json", "w") as f:
    json.dump(weightedPaperCountMap, f)
with open(f"{path_to_mapping}/coWeightedPaperCountMap.json", "w") as f:
    json.dump(coWeightedPaperCountMap, f)
with open(f"{path_to_mapping}/coPaperCountMap.json", "w") as f:
    json.dump(coPaperCountMap, f)