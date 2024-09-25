import pandas as pd 
import numpy as np
import sys

from bertopic import BERTopic
from scipy.spatial import distance
from datetime import datetime


suffix = f'-{sys.argv[1]}'
sample = len(sys.argv) > 2 and sys.argv[2] == 'sample'
field = 'fellowVSNon'
authorType = 'autoTop'
dirMid = f'{field}{suffix}'
outDir = f'./output/{dirMid}'
modelPath = f'./model/{dirMid}'
print(f'field: {field}, authorType: {authorType}, s')
originDf = pd.read_csv(f'./src/papers_{field}_{authorType}.csv')
df = originDf.dropna()

print(f'origin papers count: {len(originDf)}')

df['documents'] = (df['title'] + '. ') * 3 +  df['abstract']
paperKey = df.paperID.tolist()
docs = df.documents.tolist()

topic_model = BERTopic.load(f'{modelPath}/topicModel')
originDf['documents'] = (originDf['title'] + '. ') * 3 +  originDf['abstract'].apply(lambda x: x if type(x) == str else '')
originPaperKey = originDf.paperID.tolist()
originDocs = originDf.documents.tolist()

docEmbeddings = topic_model._extract_embeddings(originDocs,
                                                method="document",
                                                verbose=False)
topicEmbeddings = topic_model.topic_embeddings_[topic_model._outliers:]

# distances = np.array([np.linalg.norm(docEmbedding - topicEmbeddings, axis=1) for docEmbedding in docEmbeddings])
# dfDis = pd.DataFrame(distances, columns=[f'topic_{i}' for i in range(len(topicEmbeddings))])
# dfDis.round(3)
# dfDis['paperID'] = originPaperKey
# dfDis = dfDis[['paperID'] + [f'topic_{i}' for i in range(len(topicEmbeddings))]]
# dfDis.to_csv(f'{outDir}/paperIDEuDisDist.csv', index=False)

def mahalanobis_distance(vector, vectors, cov_inv):
    """
    Calculate the Mahalanobis distance between a vector and a group of vectors.
    
    Parameters:
    vector : array_like
        The vector for which distances are to be calculated.
    vectors : array_like
        An array of vectors to which distances are to be calculated.
    cov_inv : array_like
        The inverse covariance matrix of the vectors.
        
    Returns:
    array_like
        An array of Mahalanobis distances between the vector and each vector in vectors.
    """
    # Calculate the Mahalanobis distance between the vector and each vector in vectors
    distances = []
    for v in vectors:
        distances.append(distance.mahalanobis(v, vector, cov_inv))
    return distances

def myPrint(msg):
    print('[' + datetime.now().isoformat(sep=' ', timespec='milliseconds') + ']' + str(msg) + '\n')

myPrint('start calculate Mahalanobis distance, sample:', sample)
if sample:
    cov_matrix
else:
    cov_matrix = np.cov(docEmbeddings.T)  # Calculate covariance matrix
myPrint('calculate covariance matrix finished')
cov_inv = np.linalg.inv(cov_matrix)
myPrint('calculate inverse covariance matrix finished')
mhDistances = []
for docEmbedding in docEmbeddings:
    mhDistances.append([distance.mahalanobis(docEmbedding, topicEmbedding, cov_inv) for topicEmbedding in topicEmbeddings])
myPrint('calculate Mahalanobis distance finished')

mhDistances = np.array(mhDistances)
dfDis = pd.DataFrame(mhDistances, columns=[f'topic_{i}' for i in range(len(topicEmbeddings))])
dfDis.round(3)
dfDis['paperID'] = originPaperKey
dfDis = dfDis[['paperID'] + [f'topic_{i}' for i in range(len(topicEmbeddings))]]
dfDis.to_csv(f'{outDir}/paperIDMHDisDist.csv', index=False)

# probs = topic_model.probabilities_
# dfProbs = pd.DataFrame(probs, columns=[f'topic_{i}' for i in range(probs.shape[1])])
# dfProbs = dfProbs.round(4)
# dfProbs.replace(0.0, np.nan, inplace=True)
# dfProbs['paperID'] = paperKey
# dfProbs = dfProbs[['paperID'] + [f'topic_{i}' for i in range(probs.shape[1])]]
# dfProbs.to_csv(f'{outDir}/paperIDDist-Probs.csv', index=False)

# topic_distr, _ = topic_model.approximate_distribution(docs)
# dfTopicDist = pd.DataFrame(topic_distr, columns=[f'topic_{i}' for i in range(topic_distr.shape[1])])
# dfTopicDist = dfTopicDist.round(4)
# dfTopicDist.replace(0.0, np.nan, inplace=True)
# dfTopicDist['paperID'] = paperKey
# dfTopicDist = dfTopicDist[['paperID'] + [f'topic_{i}' for i in range(topic_distr.shape[1])]]
# dfTopicDist.to_csv(f'{outDir}/paperIDDist-tfidf-AD.csv', index=False)

# topic_distr, _ = topic_model.approximate_distribution(docs, useMean=True)
# dfTopicDist = pd.DataFrame(topic_distr, columns=[f'topic_{i}' for i in range(topic_distr.shape[1])])
# dfTopicDist = dfTopicDist.round(4)
# dfTopicDist.replace(0.0, np.nan, inplace=True)
# dfTopicDist['paperID'] = paperKey
# dfTopicDist = dfTopicDist[['paperID'] + [f'topic_{i}' for i in range(topic_distr.shape[1])]]
# dfTopicDist.to_csv(f'{outDir}/paperIDDist-embedding-AD.csv', index=False)