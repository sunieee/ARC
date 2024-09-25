import pandas as pd
import numpy as np

import sys
import os
import json
import bisect

from bertopic import BERTopic

field = 'VCG'
authorType = 'autoTop'

remapSet = set([-1, 12])
remapLen = len(remapSet) + 1 

# field = sys.argv[1]
modelpath = f'./model/{field}/topicModel'
outDir = f'./output/{field}'

if not os.path.exists(modelpath):
    print('model not exists!')
    sys.exit(-1)

# load documents and process like learning
originDf = pd.read_csv(f'./src/papers_{field}_{authorType}.csv')
df = originDf.dropna()
df['documents'] = (df['title'] + '. ') * 3 +  df['abstract']
paperKey = df.paperID.tolist()
docs = df.documents.tolist()

# load topic model
topic_model = BERTopic.load(modelpath)

topic_distr, topic_token_distr = topic_model.approximate_distribution(docs, calculate_tokens=True)

# get top probability topic metrics
maxMetrics = np.argsort(-topic_distr, axis=1)[:, :remapLen]
with open(f'{outDir}/paperID2topic.json', 'r') as f:
    paperID2topicDt = json.load(f)

# get old topic
dfTopics = pd.read_csv(f'{outDir}/topic_count_name.csv')
dfTopics = dfTopics[['Topic', 'Count', 'Name']]

# remap new topics and set the topic index to final index
paperIDIdxDt = {}
for idx, paperID in enumerate(paperKey):
    paperIDIdxDt[str(int(paperID))] = idx

remapCnt = dfTopics.Count.values.tolist()
sortedRemapList = list(remapSet)
sortedRemapList.sort()

for _, row in originDf.iterrows():
    strPaperID = str(int(row['paperID']))
    oldIdx = paperID2topicDt[strPaperID]
    if oldIdx in remapSet:
        if paperIDIdxDt.__contains__(strPaperID):
            paperIdx = paperIDIdxDt[strPaperID]
            similarTopics = maxMetrics[paperIdx]
        else:        
            title = row['title']
            similarTopics, similarity = topic_model.find_topics(title, top_n=remapLen)
        
        for i in range(remapLen):
            topicIdx = int(similarTopics[i])
            if not topicIdx in remapSet:
                
                gapTopicIdx = bisect.bisect_left(sortedRemapList, topicIdx) - 1
                
                # save the final topic in the paperID2topicDt
                paperID2topicDt[strPaperID] = topicIdx - gapTopicIdx
                
                # save the mid topic in remapCnt and remove remap set in the final
                remapCnt[topicIdx] += 1
                break
    else:
        # save the final topic in the paperID2topicDt
        paperID2topicDt[strPaperID] = oldIdx - bisect.bisect_left(sortedRemapList, oldIdx) + 1

# save paperID2topicDt
with open(f'{outDir}/paperID2topic.json', 'w') as f:
    f.write(json.dumps(paperID2topicDt, indent=2, ensure_ascii=False))
    
descRemoveList = list(remapSet - set([-1]))
descRemoveList.sort(reverse=True)

def removeDfRow(df, removeList):
    for idx in removeList:
        df = df.drop(idx)
    return df

def removeListElement(ll, removeList):
    for idx in removeList:
        ll.pop(idx)
    return ll

# save dfTopics
remapCntNew = removeListElement(remapCnt, descRemoveList)
infoList = []
for _, row in dfTopics.iterrows():
    topic, count, name = row
    gap = bisect.bisect_left(sortedRemapList, topic) - 1
    topic = topic - gap
    name = '_'.join([str(topic)] + name.split('_')[1:])
    infoList.append([topic, remapCntNew[topic], name])

df = pd.DataFrame(infoList, columns=['Topic', 'Count', 'Name'])
df.to_csv(f'{outDir}/topic_count_name.csv', index=False)

# save topic location
df = pd.read_csv(f'{outDir}/topic_location.csv')
df = removeDfRow(df, descRemoveList)
df.to_csv(f'{outDir}/topic_location.csv', index=False)

# save topic word prob raw

with open(f'{outDir}/topic_word_prob_raw.json', 'r') as f:
    topic_word_prob = json.load(f)

topic_word_prob = removeListElement(topic_word_prob, descRemoveList)
with open(f'{outDir}/topic_word_prob_raw.json', 'w') as f:
    f.write(json.dumps(topic_word_prob, indent=2, ensure_ascii=False))
    
os.system(f'bash geneOthers.sh {field}')
