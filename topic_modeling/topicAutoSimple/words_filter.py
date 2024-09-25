# 尽量选够5个word，超过5个word的topic需要过滤prob<0.01的word
import json
import os
import sys
if len(sys.argv) != 2:
    print("format: python filter_words.py field")
    sys.exit()
field = sys.argv[1]

directory = sys.path[0] + "/output/" + field
with open(os.path.join(directory, "topic_word_prob_manual.json"), 'r') as f:
    data = json.load(f)

res = []
for wordProbDict in data:
    newWordProbDict = {}
    for word, prob in wordProbDict.items():
        if (len(newWordProbDict) < 5) or (len(newWordProbDict) >= 5 and prob >= 0.01):
            newWordProbDict[word] = prob
    res.append(newWordProbDict)

data = json.dumps(res, indent=4)
with open(os.path.join(directory, "topic_word_prob.json"), "w") as f:
    f.write(data)