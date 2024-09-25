import pandas as pd
import json
import os
import sys
if len(sys.argv) != 2:
    print("format: python merge_words.py field")
    sys.exit()
field = sys.argv[1]

# import spacy
# nlp = spacy.load("en_core_web_sm")
# df = pd.read_csv("topicName.csv", sep=',')
# for i, row in df.iterrows():
#     s = ' '
#     words = s.join(row.values.tolist())
#     doc = nlp(words)

#     for token in doc:
#         print(token.text, token.lemma_)

import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from nltk.stem.porter import PorterStemmer  
stemmer = PorterStemmer()  

def get_wordnet_pos(tag):
    if tag.startswith('J'):
        return wordnet.ADJ
    elif tag.startswith('V'):
        return wordnet.VERB
    elif tag.startswith('N'):
        return wordnet.NOUN
    elif tag.startswith('R'):
        return wordnet.ADV
    else:
        return None

wnl = WordNetLemmatizer()
# lemmatize nouns
# print(wnl.lemmatize('cars'))
directory = sys.path[0] + "/output/" + field
with open(os.path.join(directory, "topic_word_prob_raw.json"), "r") as f:
    data = json.load(f)
unused_words = ["the", "of", "and", "for", "on", "we", "to", "that", "this", "in", "was", "were", 'a', "off"]
lst = []
for words in data:
    dic = {}
    word = words.keys() # 当前topic的所有word
    word_attrs = [nltk.pos_tag([w])[0] for w in word]   # 所有word的词性(word, 名词/形容词/...)
    # print(word_attrs)
    for t1, t2 in zip(words.items(), word_attrs):
        word, prob = t1
        tag = t2[1]
        if word not in unused_words:
            pos = get_wordnet_pos(tag) or wordnet.NOUN  # word的词性
            # word = stemmer.stem(word)
            res = wnl.lemmatize(word=word, pos=pos) # 提取的词根
            if res == "visualisation":
                res = "visualization"
            cnt = 0
            for word_key in dic.keys():
                if res in word_key:
                    dic[word_key] += prob
                    break
                cnt += 1
            if cnt == len(dic.keys()):
                dic[res] = prob
    dic = dict(sorted(dic.items(), key=lambda x:x[1], reverse=True))
    lst.append(dic)
    # print(len(dic), end='\t')
# print()
data = json.dumps(lst, indent=4, separators=(',', ': '))
with open(os.path.join(directory, "topic_word_prob_merged.json"), "w") as f:
    f.write(data)