import pandas as pd
from tqdm import tqdm

file = 'fellow'
folder = 'fellows'
df = pd.read_csv(f'{folder}/{file}CS.csv', dtype={'authorID': str})
unique_authors = df['original'].unique().tolist()

df = df[df['CSPaperRatio'] >= 0.45]
# original 为空('')的，用name填充
df.loc[df['original'] == '', 'original'] = df.loc[df['original'] == '', 'name']
df.loc[df['original'].isnull(), 'original'] = df.loc[df['original'].isnull(), 'name']

def has_same_middlename(name1, name2):
    if len(name1.split(' ')) == 2:
        return True
    if len(name1.split(' ')) == 3 and len(name2.split(' ')) == 3:
        return name1.split(' ')[1][0] == name2.split(' ')[1][0]
    return False


for original in tqdm(unique_authors):
    if len(df[df['original'] == original]) > 1:
        # 如果存在original = name，则保留相等的行，删掉其他行 
        equal_rows = df[(df['original'] == original) & (df['original'] == df['name'])]
        if len(equal_rows) > 0:
            df = df[df['original'] != original]
            df = df.append(equal_rows)
        
        # 取相同作者中CSPaperCount最大的行，及所有CSPaperCount >= 20%最大的行
        max_cspapercount = df[df['original'] == original]['CSPaperCount'].max()
        max_cspapercount_rows = df[(df['original'] == original) & (df['CSPaperCount'] >= max_cspapercount * 0.2)]
        if len(max_cspapercount_rows) > 5:
            # 数量超过3个则取CSCitationCount最大的5个
            max_cspapercount_rows = max_cspapercount_rows.sort_values(by=['CSCitationCount'], ascending=False).iloc[:5]
        df = df[df['original'] != original]
        df = df.append(max_cspapercount_rows)

        # 取相同作者中CSCitationCount最大的行的name，如果split=3，则添加middlename首字母相同的行
        max_row = df[df['original'] == original].sort_values(by=['CSCitationCount'], ascending=False).iloc[0]
        max_name = max_row['name']
        split = len(max_name.split(' '))
        if split == 3:
            same_name_rows = df[(df['original'] == original) & (df['name'].apply(lambda x: has_same_middlename(x, max_name)))]
            df = df[df['original'] != original]
            df = df.append(same_name_rows)



# original,authorID,name,year,CitationCount,PaperCount,CSPaperCount,CSCitationCount
df = df[['original', 'authorID', 'name', 'year', 'hIndex', 'PaperCount', 'CitationCount', 'CSPaperCount', 'CSCitationCount', 'CSPaperRatio', 'CSCitationRatio']]
# df.rename(columns={'PaperCount': 'PaperCount'}, inplace=True)
print(len(df['original'].unique()))

# 删除df中original相同的行
duplicate_original = df[df.duplicated(subset=['original'], keep=False)]
df = df.drop(duplicate_original.index)

print(len(df))

df.to_csv(f'{folder}/{file}CS_unique.csv', index=None)