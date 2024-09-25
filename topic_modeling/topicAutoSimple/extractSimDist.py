from bertopic import BERTopic
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity

import pandas as pd
import numpy as np

import sys
import os
import json


def myPrint(msg):
    print('[' + datetime.now().isoformat(sep=' ', timespec='milliseconds') + ']' + str(msg) + '\n')

if len(sys.argv) != 2:
    myPrint("Format: python extractSimDist $field")
    sys.exit()

field = sys.argv[1]
modelPath = f"./model/{field}/topicModel"

if not os.path.exists(modelPath):
    myPrint(f"Error: model doesn't exist: \"{modelPath}\"")
    sys.exit()
myPrint('start to load model')
topicModel = BERTopic.load(modelPath)
myPrint('load model finished')
authorType = 'autoTop'
outDir = f"./output/{field}"
originDf = pd.read_csv(f'./src/papers_{field}_{authorType}.csv')
df = originDf.dropna()

myPrint(f'field: {field}, authorType: {authorType}')

originDf['documents'] = (originDf['title'] + '. ') * 3 +  originDf['abstract'].apply(lambda x: x if type(x) == str else '')
originPaperKey = originDf.paperID.tolist()
originDocs = originDf.documents.tolist()

df['documents'] = (df['title'] + '. ') * 3 +  df['abstract']
paperKey = df.paperID.tolist()
docs = df.documents.tolist()

docEmbeddings = topicModel._extract_embeddings(originDocs,
                                               method="document",
                                               verbose=False)

myPrint('docEmbeddings finished')
docSim = cosine_similarity(docEmbeddings, topicModel.topic_embeddings_[topicModel._outliers:])

dfSim = pd.DataFrame(docSim, columns=[f'topic_{i}' for i in range(docSim.shape[1])])
dfSim['paperID'] = originPaperKey
dfSim = dfSim.round(3)
dfSim.replace(0.0, np.nan, inplace=True)
dfSim['paperID'] = dfSim['paperID'].astype(int)
dfSim = dfSim[['paperID'] + [f'topic_{i}' for i in range(docSim.shape[1])]]
dfSim.to_csv(f'{outDir}/paperIDDistribution.csv', index=False)

max_idx = np.argmax(docSim, axis=1)
paperIDMaxIdxDt = {}
for paperID, idx in zip(originPaperKey, max_idx):
    paperIDMaxIdxDt[str(int(paperID))] = idx

doc_info = topicModel.get_document_info(docs)
doc_topic_info = doc_info["Topic"].values
doc_info.drop(columns=['Document', 'Name', 'Top_n_words', 'Probability', 'Representative_document'], inplace=True)
# doc_info['max_idx'] = max_idx

# 打印TopicID对应的Count和Name（去掉TopicID=-1，因为它都是一些to the and之类的topic）
df_topics = topicModel.get_topic_info()
df_topics = df_topics[['Topic', 'Count', 'Name']]
df_topics.to_csv(f'{outDir}/topic_count_name_N1.csv', index=False)

df_topics = df_topics.loc[df_topics.Topic != -1, :]

paperIDIdxDt = {}
for idx, paperID in enumerate(paperKey):
    paperIDIdxDt[str(int(paperID))] = idx

paperID2topicDt = {}

remapCnt = df_topics.Count.values.tolist()

for _, row in originDf.iterrows():
    strPaperID = str(int(row['paperID']))
    if paperIDIdxDt.__contains__(strPaperID):
        idx = paperIDIdxDt[strPaperID]
        topicIdx = int(doc_topic_info[idx])
        if topicIdx == -1:
            topicIdx = int(max_idx[paperIDMaxIdxDt[strPaperID]])
            remapCnt[topicIdx] += 1
    else:
        topicIdx = int(max_idx[paperIDMaxIdxDt[strPaperID]])
        remapCnt[topicIdx] += 1
        
    paperID2topicDt[strPaperID] = topicIdx

cntSeries = pd.Series(remapCnt, name='Count', dtype=int)
df_topics.columns = ['Topic', 'Count0', 'Name']
df_topics.reset_index(drop=True, inplace=True)
df_topics = pd.concat([df_topics, cntSeries], axis=1)
df_topics = df_topics[['Topic', 'Count', 'Name']]

df_topics['Topic'].astype(int)
df_topics['Count'].astype(int)
df_topics.to_csv(f'{outDir}/topic_count_name.csv', index=False)

nowDate = str(datetime.now().isoformat(sep=' ', timespec='milliseconds'))[2:10]
nowDate = nowDate.replace('-', '')
df_topics.to_csv(f'{outDir}/topic_{field}_{len(df_topics)}_{nowDate}.csv', index=False)

# 打印所有topic中每个topic中10个word-prob关系
data = []
for i in range(topicModel.get_topic_info().shape[0] - 1):
    topicID = topicModel.get_topic(i)      # 每个topic由10个word构成，打印第i个topic中每个(word, prob)
    topic_word_prob = {word: prob for word, prob in topicID}
    data.append(topic_word_prob)
data = json.dumps(data, indent=4, separators=(',', ': '))
with open(f'{outDir}/topic_word_prob_raw.json', "w") as f:
    f.write(data)

# 可视化topic分布2d图
# fig = topic_model.visualize_topics(output_path=(sys.path[0] + "/output/" + field))
fig = topicModel.visualize_topics(output_path=f'{sys.path[0]}/output/{field}')
fig.write_html(f'{outDir}/topic_distribution.html')

with open(f'{outDir}/paperID2topic.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(paperID2topicDt, indent=4))
    
myPrint('extractSimDist.py run finished')