# 将topic的word按照处理过的文件重新拼接
# topic-location.csv文件最后一行有问题，要手动处理完才能运行该程序
import pandas as pd
import json
import os
import sys
if len(sys.argv) != 2:
    print("format: python filter_words.py $field")
    sys.exit()
field = sys.argv[1]
directory = sys.path[0] + "/output/" + field

df_topic = pd.read_csv(os.path.join(directory, "topic_count_name.csv"), sep=',')
df_location = pd.read_csv(os.path.join(directory, "topic_location.csv"), sep=',')
df = pd.concat([df_topic, df_location], axis=1)
# print(df)
topic1 = df.values.tolist()

# 0715: 更改不再进行manual和filter，topic_word_prob
with open(os.path.join(directory, "topic_word_prob_merged.json"), 'r') as f:
    topic2 = json.load(f)

lst = []
for t1, t2 in zip(topic1, topic2):
    t1[2] = '_'.join(t2.keys())
    lst.append(t1)

df = pd.DataFrame(lst, columns=["Topic", "Count", "Name", 'x', 'y'])
df.to_csv(os.path.join(directory, "topic.csv"), sep=',', index=False)