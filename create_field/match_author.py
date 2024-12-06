from gensim import utils
import pandas as pd
from Levenshtein import ratio as levenSim
import os
import unicodedata
import re
import numpy
from tqdm import tqdm
import time
import json
import pandas as pd
import gensim
from gensim.parsing.preprocessing import preprocess_string
import multiprocessing
from utils import field, cursor, conn, engine, NumpyEncoder, topN
import pandas as pd
import re
from utils import topN


finals = ['a','o','e','i','u','v','ai','ei','ui','ao','ou','iu','ie','ue','ve','an','en','in','un','vn','ang','eng','ing','ong','iang','uang','uan','ua','ian']
initials = ['b','p','m','f','d','t','n','l','g','k','h','j','q','x','zh','ch','sh','r','z','c','s','y','w']
pinyins = ['zhi','chi','shi','ri','zi','ci','si','yi','wu','yu','ye','yue','yuan','yin','yun','ying']
for initial in initials:
    for final in finals:
        pinyin=initial+final
        pinyins.append(pinyin)
pinyins=set(pinyins)


gensim.parsing.preprocessing.STOPWORDS = set()
def strip_short2(s, minsize=1):
    s = utils.to_unicode(s)         #hajičová在数据库里是hajicova,因此没匹配上'Eva Hajičová'
    s = ''.join(char for char in unicodedata.normalize('NFKD', s) if not unicodedata.combining(char))
    def remove_short_tokens(tokens, minsize):
        return [token for token in tokens if len(token) >= minsize]
    return " ".join(remove_short_tokens(s.split(), minsize))
gensim.parsing.preprocessing.DEFAULT_FILTERS[6]=strip_short2
del gensim.parsing.preprocessing.DEFAULT_FILTERS[-1]

'''compare name1 and name2, return similarity'''
def compare_name(n1,n2, levensimrate=0.7):
    n1_tb = preprocess_string(re.sub('[^\s\w]', "", n1))
    n2_tb = preprocess_string(re.sub('[^\s\w]', "", n2))
    n1_tb=sorted(n1_tb,key = lambda i:len(i),reverse=True)
    n2_tb = sorted(n2_tb, key=lambda i: len(i), reverse=True)
    if set(n1_tb).issubset(set(n2_tb)) or set(n2_tb).issubset(set(n1_tb)):
        return 1.0
    n1_py=[]
    n2_py=[]
    pinyinflag1=True
    pinyinflag2=True
    for word in n1_tb:
        tb,flag=parse_pinyin2(word)
        pinyinflag1 = (pinyinflag1 and flag) if len(word)!=1 else pinyinflag1
        n1_py.extend(tb)
    for word in n2_tb:
        tb, flag = parse_pinyin2(word)
        pinyinflag2 = (pinyinflag2 and flag) if len(word)!=1 else pinyinflag2
        n2_py.extend(tb)
    pinyinflag = pinyinflag1 and pinyinflag2
    if (len(n1_py)!=len(n1_tb) or len(n2_py)!=len(n2_tb)) and len(n1_py)==len(n2_py) and set(n1_py)==set(n2_py) and pinyinflag:
        return 1.0
    penalty_1=len(n1_tb)
    penalty_2=len(n2_tb)
    n1_rest=n1_tb.copy()
    for word1 in n1_tb:
        rmflag = ''
        for word2 in n2_tb:
            if min(len(word1),len(word2)) == 1 and (word1.startswith(word2) or word2.startswith(word1)) and pinyinflag1==pinyinflag2:
                rmflag=word2
                penalty_1-=0.9
                penalty_2-=0.9
                break
            elif parse_pinyin2(word1)[1] and parse_pinyin2(word2)[1]:
                if word1==word2:
                    penalty_1 -= 1
                    penalty_2 -= 1
                pass
            elif levenSim(word1, word2) >= levensimrate:
                rmflag=word2
                penalty_1-=levenSim(word1,word2)
                penalty_2-=levenSim(word1,word2)
                break
        try:
            n2_tb.remove(rmflag)
            n1_rest.remove(word1)
        except:
            pass
    n2_rest = n2_tb.copy()
    restword1=''.join(n1_rest)
    restword2=''.join(n2_rest)
    if levenSim(restword1, restword2) >= 0.9 and (not parse_pinyin2(restword1)[1] or not parse_pinyin2(restword2)[1]):
        penalty_1 -= levenSim(restword1, restword2)
        penalty_2 -= levenSim(restword1, restword2)
    penalty=min(penalty_1,penalty_2)+1
    return min(1.0, float(1/penalty))

def compare_nametb(tb1,tb2):
    tb1_dict={ele:[0,[]] for ele in tb1}
    tb2_dict={ele:[0,[]] for ele in tb2}
    for n1 in tb1:
        for n2 in tb2:
            similarity=compare_name(n1, n2)

            if similarity>tb1_dict[n1][0]:
                tb1_dict[n1][1] = [n2]
                tb1_dict[n1][0] = similarity
            elif similarity==tb1_dict[n1][0] and n2 not in tb1_dict[n1][1]:
                tb1_dict[n1][1].append(n2)

            if similarity>tb2_dict[n2][0]:
                tb2_dict[n2][1] = [n1]
                tb2_dict[n2][0] = similarity
            elif similarity==tb2_dict[n2][0] and n1 not in tb2_dict[n2][1]:
                tb2_dict[n2][1].append(n1)
    return tb1_dict,tb2_dict

def dump_matchdict(path,matchdict):
    data=[]
    for line in matchdict.items():
        data.append([line[1][0],line[0],'|'.join(line[1][1])])
    df=pd.DataFrame(data,columns=["similarity","host_name","matcher_name"])
    df=df.sort_values(by="similarity",ascending=False)
    df.to_csv(path+'.csv',index=None)
    return df

def parse_pinyin2(word):
    w=word
    output=[]
    flag_pinyin=True
    for i in range(1,3):
        for i in range(min(6,len(w)),1,-1):
            if w[0:i] in pinyins:
                output.append(w[0:i])
                break
        if len(output)==0:
            flag_pinyin = False
            output = [word]
            break
        elif i==len(w):
            break
        else:
            w=w[i:]
    if ''.join(output) != word:
        output=[word]
        flag_pinyin = False
    return output,flag_pinyin

def levenshtein_distance(s1, s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1
    distances = range(len(s1) + 1)
    for index2, char2 in enumerate(s2):
        new_distances = [index2 + 1]
        for index1, char1 in enumerate(s1):
            if char1 == char2:
                new_distances.append(distances[index1])
            else:
                new_distances.append(1 + min((distances[index1], distances[index1 + 1], new_distances[-1])))
        distances = new_distances

    return distances[-1]

def test():
    # s1 = 'J. J. van Wijk'
    # s2 = 'Jarke J. van Wijk'
    s1 = "kitten"
    s2 = "sitting"

    t = time.time()
    for i in range(100):
        compare_name(s1, s2)
    print('time', time.time() - t)

    t = time.time()
    for i in range(100):
        levenshtein_distance(s1, s2) # / max(len(s1), len(s2))
    print('time', time.time() - t)

def compute_levenshtein(pair):
    i, j = pair
    return pair, levenshtein_distance(author_names[i], author_names[j]) / (author_names_len[i] + author_names_len[j])

def process_group(group):
    ix1, ix2 = group[0]
    similarity = compare_name(author_names[ix1], author_names[ix2])
    return {
        'id1': author_ids[ix1],
        'id2': author_ids[ix2],
        'name1': author_names[ix1],
        'name2': author_names[ix2],
        'lev_dis': group[1],
        'similarity': similarity
    }


def valid_word(word):
    # 验证首字母大写，后续字母小写
    return word[0].isupper() and word[1:].islower()

# 验证拼音音节，深度优先搜索
def valid_pinyins(word):
    if word in pinyins:
        return True
    for i in range(1, len(word)):
        if word[:i] in pinyins and valid_pinyins(word[i:]):
            return True
    return False

# 判断是否为中文拼音名字
def is_chinese_name(name):
    """
    判断名字是否可能为中文拼音名字。
    规则：
    - 名字由2-4个单词组成（姓和名）。
    - 每个单词为合法拼音音节。
    """
    words = name.split()
    if len(words) != 2:
        return False
    if not valid_word(words[1]) or not valid_word(words[0]):
        return False
    name0 = words[0].lower()
    name1 = words[1].lower()
    if not valid_pinyins(name1) or not valid_pinyins(name0):
        return False
    return True



# 在融合作者时，我们只关心top_authors，后面的人不重要，节省计算量
df_top = pd.read_csv(f'out/{field}/top_authors.csv')
num = min(len(df_top), 10000)

# 读取 CSV 文件
df = pd.read_csv(f'out/{field}/csv/authors.csv')
df = df.sort_values(by='hIndex_field', ascending=False).reset_index(drop=True)
# 获取特定位置（假设为变量 'num'）的 hIndex_field 值
hIndex0 = df.loc[num, 'hIndex_field']
print('MIN hIndex:', hIndex0)
assert hIndex0 > 0
# 筛选出 hIndex_field 大于等于 hIndex0 的行
df = df[df['hIndex_field'] >= hIndex0]
# 将 'authorID' 列的数据类型转换为字符串
df['authorID'] = df['authorID'].astype('str')

print(df.shape)
print(df.head())

'''
(5984, 9)
    authorID   rank               name  PaperCount  CitationCount  PaperCount_field  authorRank  CitationCount_field  hIndex_field
0  1166287365  14614    Dorian Liepmann         141           4527                12        4864                  606           NaN
1  2032738043  13085  Peter Brusilovsky         523          19831                71         194                 1147          20.0
2  2040206780  14993       Amy A. Gooch          52           2934                14        3642                  236           NaN
3  2073393117  17045      Rachid Gherbi          39            146                16        3016                   48           NaN
4  2094673208  17259          Ilmi Yoon          33            170                12        5075                  105           NaN
'''

author_ids = df['authorID']
author_names = df['name']
author_names_len = [len(name) for name in author_names]
lev_file = f'out/{field}/lev.json'

# if os.path.exists(lev_file):
#     with open (lev_file, 'r') as f:
#         lev_lis = json.load(f)
# else:
lev_dic = {}
pairs = []
print('filtering pairs...')
for i in tqdm(range(len(author_names))):
    for j in range(len(author_names)):
        if i < j:
            la = author_names_len[i]
            lb = author_names_len[j]
            if abs(la - lb) / (la + lb) <= 0.1: # and abs(math.log2(la) - math.log2(lb)) < 1:
                pairs.append((i,j))

# for i, j in tqdm(pairs):
#     lev_dic[(i,j)] = levenshtein_distance(author_names[i], author_names[j]) / (len(author_names[i]) + len(author_names[j]))
# 使用多进程计算编辑距离
print(f'computing levenshtein distances...({int(len(pairs)/100000)}s)', len(pairs))
# 为了最大化并行计算的效率，进程的数量设置为与CPU核心数相同是一个好的起点。
# 但是，最佳的进程数量可能还取决于具体的任务和其他系统负载
with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    # 创建一个进度条
    # pbar = tqdm(total=len(pairs))
    # # 使用imap或imap_unordered，并迭代结果来更新进度条
    # results = []
    # for result in pool.imap(compute_levenshtein, pairs):
    #     results.append(result)
    #     pbar.update()
    # pbar.close()

    # 有进度条只有33%，没有进度条能够达到100%CPU利用率，20min程序总共1min跑完
    results = pool.map(compute_levenshtein, pairs)
lev_dic = dict(results)

# sort lev_dic by values
lev_lis = list(sorted(lev_dic.items(), key=lambda item: item[1]))
lev_lis = [g for g in lev_lis if g[1] <= 0.1]

with open(lev_file, 'w') as f:
    json.dump(lev_lis, f, cls=NumpyEncoder)

groups = pd.DataFrame(columns=['id1', 'id2', 'name1', 'name2', 'lev_dis', 'similarity'])



print(f'comparing names on {len(lev_lis)} pairs...(30s)')
with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    results = pool.map(process_group, lev_lis)

# 将结果合并到groups
for result in results:
    groups.loc[len(groups)] = result


# groups.to_csv(f'out/{field}/groups.csv', encoding='UTF-8', index=False)
match = groups[groups['similarity'] > 0.96]
match.to_csv(f'out/{field}/match.csv', index=False)

# 删除 name1 是中文名字的行
match = match[~match['name1'].apply(is_chinese_name)]
match = match[~match['name2'].apply(is_chinese_name)]

match.to_csv(f'out/{field}/match_modify.csv', index=False)