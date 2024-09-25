import pandas as pd
import numpy as np

from bertopic import BERTopic
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import MinMaxScaler
from umap import UMAP
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
from datetime import datetime

import sys
import os
import transformers
import time
import random
import json


def myPrint(msg):
    print('[' + datetime.now().isoformat(sep=' ', timespec='milliseconds') + ']' + str(msg) + '\n')
    
if len(sys.argv) != 5:
    myPrint("Format: python bertopic_title_abstract $field $authorType $minTopicSize $minTopicCount")
    sys.exit()

field, authorType, minTopicSize, minTopicCount = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])

outDir =f"./output/{field}"
modelPath = f"./model/{field}"

os.makedirs(outDir, exist_ok=True)
os.makedirs(modelPath, exist_ok=True)

myPrint(f'field: {field}, minTopicSize: {minTopicSize}')

paperKey = []
docs = []

originDf = pd.read_csv(f'./src/papers_{field}_{authorType}.csv')

myPrint(f'origin papers count: {len(originDf)}')
df = originDf.dropna(subset=['abstract'])
myPrint(f'papers count with abstract: {len(df)}')

df['documents'] = (df['title'] + '. ') * 3 +  df['abstract']

# import re
# def preprocess_text(text, stopword_substrings):
#     # 移除包含特定子串的词
#     for substring in stopword_substrings:
#         text = re.sub(r'\b\w*' + re.escape(substring) + r'\w*\b', '', text, flags=re.IGNORECASE)
#     # 移除多余的空格
#     text = re.sub(r'\s+', ' ', text).strip()
#     return text
# df['documents'] = df['documents'].apply(lambda x: preprocess_text(x, custom_stopwords_substrings))

CUSTOM_STOP_WORDS = ['graph', 'network', 'visualization']
ENGLISH_STOP_WORDS = ["a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", 
     "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", 
     "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", 
     "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", 
     "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", 
     "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", 
     "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven", 
     "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", 
     "except", "few", "fifteen", "fifty", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", 
     "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", 
     "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", 
     "how", "however", "hundred", "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", 
     "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", 
     "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", 
     "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", 
     "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", 
     "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", 
     "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", 
     "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", 
     "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", 
     "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", 
     "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", 
     "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", 
     "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", 
     "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", 
     "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves"]

if os.environ.get('model_path'):
    embedding_model = SentenceTransformer(os.environ.get('model_path'))
else:
    embedding_model = "paraphrase-MiniLM-L12-v2"
print(f'embedding_model: {embedding_model}')

paperKey = df.paperID.tolist()
docs = df.documents.tolist()

if os.path.exists(f'{modelPath}/topicModel'):
    myPrint("model exists!")
    topic_model = BERTopic.load(f'{modelPath}/topicModel')
    myPrint('load model finished')
else:
# 预训练模型 all-mpnet-base-v2 paraphrase-MiniLM-L12-v2
# sentence_model = SentenceTransformer("model/all-mpnet-base-v2")
# embeddings = sentence_model.encode(docs, show_progress_bar=True)
# topic_model = BERTopic(verbose=True, min_topic_size=120)
# topics, probs = topic_model.fit_transform(docs, embeddings)
    myPrint("model doesn't exist!")
    if os.path.exists(modelPath) == False:
        os.mkdir(modelPath)
    
    # umapModel = UMAP(random_state=2023)
    ii = 0
    while True:
        vectorizerModel = CountVectorizer(stop_words=CUSTOM_STOP_WORDS+ENGLISH_STOP_WORDS)
        topic_model = BERTopic(verbose=True, embedding_model="paraphrase-MiniLM-L12-v2", \
                               min_topic_size=minTopicSize, calculate_probabilities=True, \
                                vectorizer_model=vectorizerModel, top_n_words=20)
        # topic_model = BERTopic(verbose=True, embedding_model="paraphrase-MiniLM-L12-v2", min_topic_size=minTopicSize, calculate_probabilities=True, umap_model=umapModel, vectorizer_model=vectorizerModel)
        
        # topic_model = BERTopic(verbose=True, embedding_model="paraphrase-MiniLM-L12-v2", min_topic_size=minTopicSize, calculate_probabilities=True, umap_model=umapModel)
        
        myPrint('model learned start')
        
        topics, probs = topic_model.fit_transform(docs)
        
        myPrint(f'model learned finished, {len(topic_model.get_topic_info()) - 1} topics learned')
        if len(topic_model.get_topic_info()) > minTopicCount:
            # with open(f'{outDir}/status.txt', 'w') as f:
            #     f.write('1')
            break
        
        ii += 1
        if ii > 20:
            # with open(f'{outDir}/status.txt', 'w') as f:
            #     f.write('0')
            sys.exit(-1)

        
    topic_model.save(f'{modelPath}/topicModel')
    myPrint('model saved finished')

# probs = topic_model.probabilities_
# topicNum = probs.shape[1]
# dfProbs = pd.DataFrame(probs, columns=[f'topic_{i}' for i in range(topicNum)])
# dfProbs['paperID'] = paperKey
# dfProbs = dfProbs[['paperID'] + [f'topic_{i}' for i in range(topicNum)]]

# max_idx = np.argmax(probs, axis=1)
originDf['documents'] = (originDf['title'] + '. ') * 3 +  originDf['abstract'].apply(lambda x: x if type(x) == str else '')
originPaperKey = originDf.paperID.tolist()
originDocs = originDf.documents.tolist()

docEmbeddings = topic_model._extract_embeddings(originDocs, method="document", verbose=False)
docSim = cosine_similarity(docEmbeddings, topic_model.topic_embeddings_[topic_model._outliers:])

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

doc_info = topic_model.get_document_info(docs)
doc_topic_info = doc_info["Topic"].values
doc_info.drop(columns=['Document', 'Name', 'Top_n_words', 'Probability', 'Representative_document'], inplace=True)
# doc_info['max_idx'] = max_idx

# 打印TopicID对应的Count和Name（去掉TopicID=-1，因为它都是一些to the and之类的topic）
df_topics = topic_model.get_topic_info()
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
for i in range(topic_model.get_topic_info().shape[0] - 1):
    topicID = topic_model.get_topic(i)      # 每个topic由10个word构成，打印第i个topic中每个(word, prob)
    topic_word_prob = {word: prob for word, prob in topicID}
    data.append(topic_word_prob)
data = json.dumps(data, indent=4, separators=(',', ': '))
with open(f'{outDir}/topic_word_prob_raw.json', "w") as f:
    f.write(data)

# 可视化topic分布2d图
# fig = topic_model.visualize_topics(output_path=(sys.path[0] + "/output/" + field))
fig = topic_model.visualize_topics(output_path=f'{sys.path[0]}/output/{field}')
fig.write_html(f'{outDir}/topic_distribution.html')

with open(f'{outDir}/paperID2topic.json', 'w', encoding='utf-8') as f:
    f.write(json.dumps(paperID2topicDt, indent=4))
    
myPrint('bertopic_title_abstract.py run finished')

# 1.层次树：所有生成的topic都是叶子节点，bertopic会自己给你总结上面的topic
# hierarchical_topics = topic_model.hierarchical_topics(docs)
# tree = topic_model.get_topic_tree(hierarchical_topics)
# myPrint(tree)

# 2.Embed c-TF-IDF into 2D
# freq_df = topic_model.get_topic_freq()
# freq_df = freq_df.loc[freq_df.Topic != -1, :]   # (n, 2)矩阵，两列为Topic和Count，按Count从大到小排序
# freq_topics = sorted(freq_df.Topic.to_list())
# all_topics = sorted(list(topic_model.get_topics().keys()))
# indices = np.array([all_topics.index(topic) for topic in freq_topics])
# embeddings = topic_model.c_tf_idf_.toarray()[indices]   # topic_model.c_tf_idf_为(n, m)矩阵，m很大，所以需要用下面的UMAP降维
# embeddings = MinMaxScaler().fit_transform(embeddings)
# embeddings = UMAP(n_neighbors=2, n_components=2, metric='hellinger').fit_transform(embeddings)

# df_embeddings = pd.DataFrame(embeddings, columns=['x', 'y'])
# merge_df = pd.concat([df_topics, df_embeddings], axis=1)
# merge_df.to_csv("./output/vis-mpnet-120/topic-location.csv", sep=',', index=False)    # TODO location最后一行有问题，需要手动改s