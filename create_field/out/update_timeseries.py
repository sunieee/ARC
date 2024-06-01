import pandas as pd 
import os
from tqdm import tqdm

import multiprocessing

timeseries = pd.read_csv('timeseries.csv')

files = os.listdir('papers')

def update_paper(f):
    # print(f)
    df = pd.read_csv('papers/' + f)
    # remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    for col in ['citeStartYear','citeEndYear','totalCitationCount','citationCountByYear']:
        if col in df.columns:
            df = df.drop(columns=[col])

    df = df.merge(timeseries, on='paperID', how='left')
    df.to_csv('papers/' + f, index=False)

# multiproces_num = multiprocessing.cpu_count()
# with multiprocessing.Pool(processes=multiproces_num) as pool:
#     results = pool.map(update_paper, files)

for f in tqdm(files):
    update_paper(f)
