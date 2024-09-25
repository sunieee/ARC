# data = [
#     [0, 21, 46, 2, 9, 16, 26, 27, 22, 24, 38, 52],
#     []
# ]
from sklearn.cluster import KMeans
from sklearn import metrics

import pandas as pd
import os
import sys
if len(sys.argv) > 2:
    print("Error: python clusterTopic.py $field")
    sys.exit()
    
field = sys.argv[1]
workDir = f'./output/{field}'

numClustersLower, numClustersUpper = 8, 16

df = pd.read_csv(f'{workDir}/field.csv')
X = df[['x', 'y']].values.tolist()
y = [0 for _ in range(len(X))]

numClustersLower, numClustersUpper = 6, 30
lastScore = 0

while numClustersLower <= numClustersUpper:
    kmeans = KMeans(n_clusters=numClustersLower, random_state=0, n_init=10).fit(X, y)
    nowScore = metrics.silhouette_score(X, kmeans.labels_)
    if nowScore <= lastScore:
        break
    
    lastScore = nowScore
    numClustersLower += 1

if numClustersLower > 6:
    numClustersLower -= 1

clf = KMeans(n_clusters=numClustersLower, random_state=0, n_init=10)    # 聚类数量
clf.fit(X, y)

df_label = pd.DataFrame(clf.labels_, columns=["label"])
df_merged = pd.concat([df, df_label], axis=1)
df_merged.to_csv(f'{workDir}/field_leaves.csv', index=False)

from matplotlib import pyplot as plt
a = [n[0] for n in X]  
b = [n[1] for n in X]
plt.scatter(a, b, c=clf.labels_)
plt.savefig(f'{workDir}/topic.png')

exit(numClustersLower)