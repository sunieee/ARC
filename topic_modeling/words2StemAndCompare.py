import spacy
import en_core_web_trf
from spacy.tokenizer import Tokenizer
from spacy.tokens import Token
import sys
import json
import os
import pandas as pd
import numpy as np


# Define a function to add a custom rule for hyphens
def custom_tokenizer(nlp):
    # Create a new token to represent hyphenated words
    Token.set_extension("is_hyphenated", default=False)
    # Define a custom rule to match hyphenated words
    hyphenated_re = r'[\w]+[\-][\w]+'
    prefix_re = spacy.util.compile_prefix_regex(nlp.Defaults.prefixes)
    suffix_re = spacy.util.compile_suffix_regex(nlp.Defaults.suffixes)
    infix_re = spacy.util.compile_infix_regex([hyphenated_re])
    tokenizer = Tokenizer(nlp.vocab)
    tokenizer.token_match = None
    tokenizer.prefix_search = prefix_re.search
    tokenizer.suffix_search = suffix_re.search
    tokenizer.infix_finditer = infix_re.finditer
    return tokenizer


def getStemWordStr(wordsStr):
    """
        EXAMPLE: 
        question-answering is regarded as 3 words['question', '-', 'answering'] when nlp(question-answering), but it is probably regarded as 1 single word ['question-answering'].
        So, for each word, stem it again.
    """
    if wordsStemStrDict.__contains__(wordsStr):
        return wordsStemStrDict[wordsStr]
    
    docWord = nlp(wordsStr)
    wordList = [token.text for token in docWord]
    stemWordList = [token.lemma_ for token in docWord]
    if len(wordList) > 1:
        print(f'wrong with word {wordsStr}')

    resStr = stemWordList[0]
        
    wordsStemStrDict[wordsStr] = resStr

    return resStr


def fetchTopTopicStemName(topicName):
    names = topicName.split('_')
    stemNames = [getStemWordStr(s) for s in names[1:]]
    
    stemIdxDt = {}
    stemCnt = [0] * len(stemNames)
    for i, stemName in enumerate(stemNames):
        if stemIdxDt.__contains__(stemName):
            stemCnt[stemIdxDt[stemName]] += 1
        else:
            stemCnt[i] += 1
            stemIdxDt[stemName] = i
    
    stemArgSort = np.argsort(-np.array(stemCnt))
    
    stemList = []
    stemOriginList = []
    
    for idx in stemArgSort:
        if (stemCnt[idx] == 0):
            continue
        stemList.append(stemNames[idx])
        stemOriginList.append(names[idx + 1])
    
    return '_'.join([names[0]] + stemList), '_'.join([names[0]] + stemOriginList),


def countTopics(df):
    # 仅需讨论何时出现与之前相同的单词

    topicCnt = len(df)

    singleTopicIdxDt = {}

    # topic same with before list
    topicSWBList = [0] * topicCnt

    """
        input demo: 0_storage_method_device_system
    """
    def fetchTopTopicName(topicName):
        return topicName.split('_')[1:]

    for idx, row in df.iterrows():
        topicNames = fetchTopTopicName(row['Name'])
        cnt = 0
        for tn in topicNames:
            if singleTopicIdxDt.__contains__(tn):
                cnt += 1
            else:
                singleTopicIdxDt[tn] = idx
        
        topicSWBList[idx] = cnt
        
    return topicSWBList

    
if __name__ == '__main__':
    nlp = en_core_web_trf.load()
    nlp.tokenizer = custom_tokenizer(nlp)
    
    wordsStemStrDict = {}

    if len(sys.argv) >= 2:
        field = sys.argv[1]
    else:
        sys.exit()
        
    workDir = './topicAll/version'
    resDir = f'./results'
    
    infoList = []
    stemInfoList = []
    for dir in os.listdir(workDir):
        srcDir = f'{workDir}/{dir}/output/{field}'
        if not os.path.exists(srcDir):
            continue
    # srcDir = f'{workDir}/output/{field}'
        tmpList1 = []
        tmpList2 = []
        df0 = pd.read_csv(f'{srcDir}/topic_count_name.csv')
        df0 = df0[['Topic', 'Count', 'Name']]
        df0.to_csv(f'{srcDir}/topic_count_name_Origin.csv', index=False)
        
        for _, row in df0.iterrows():
            topic, count, name = row
            stemName, stemOriginName = fetchTopTopicStemName(name)
            tmpList1.append((topic, count, stemName))
            tmpList2.append((topic, count, stemOriginName))

        df = pd.DataFrame(tmpList2, columns=df0.columns)
        df.to_csv(f'{srcDir}/topic_count_name.csv', index=False)
        
        df = pd.DataFrame(tmpList1, columns=df0.columns)
        df.to_csv(f'{srcDir}/topic_count_stem_name.csv', index=False)
        
        topicSWBList = countTopics(df0)
        
        topicCnt = len(topicSWBList)
        mtList = [0, 0, 0, 0]
        addList = [[0, 0, 0, 0], [1, 0, 0, 0], [1, 1, 0, 0], [1, 1, 1, 0], [1, 1, 1, 1]]

        for i in range(topicCnt):
            cnt = topicSWBList[i]
            mtList = [mtList[i] + addList[cnt][i] for i in range(4)]

        mtList = [mtList[i] / topicCnt for i in range(4)]
        top3Counts = list(df0.Count.values[:3])
        infoList.append(mtList + top3Counts + [topicCnt, dir])


        topicSWBList = countTopics(df)
        topicCnt = len(topicSWBList)
        mtList = [0, 0, 0, 0]
        addList = [[0, 0, 0, 0], [1, 0, 0, 0], [1, 1, 0, 0], [1, 1, 1, 0], [1, 1, 1, 1]]

        for i in range(topicCnt):
            cnt = topicSWBList[i]
            mtList = [mtList[i] + addList[cnt][i] for i in range(4)]

        mtList = [mtList[i] / topicCnt for i in range(4)]
        top3Counts = list(df.Count.values[:3])
        stemInfoList.append(mtList + top3Counts + [topicCnt, dir])
    

    df = pd.DataFrame(infoList, columns=['MT0', 'MT1', 'MT2', 'MT3', 'topic0Count', 'topic1Count', 'topic2Count', 'topicCnt', 'dir'])
    df.to_csv(f'{resDir}/{field}Resutls.csv', index=False)
    
    df = pd.DataFrame(stemInfoList, columns=['MT0', 'MT1', 'MT2', 'MT3', 'topic0Count', 'topic1Count', 'topic2Count', 'topicCnt', 'dir'])
    df.to_csv(f'{resDir}/{field}StemResutls.csv', index=False)
    
    with open(f'{resDir}/wordsStemStrDict.json', 'w') as f:
        f.write(json.dumps(wordsStemStrDict, ensure_ascii=False, indent=2))
    