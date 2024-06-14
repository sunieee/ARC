记录各个领域数据库的处理流程：


fellows数据库已经有了多个版本：
- ACMfellow: 仅有Fellow的版本，对应ACM Fellows (1,306 authors / 362,313 papers)
- fellow: 最原始的版本，寻找Nonfellow方法有缺陷，对应 Fellow (4,014 authors / 1,087,725 papers)
- fellowTuring:  ACM Fellows & Turing Award (1,309 authors / 360,111 papers)
- fellowV1: fellowVSNon重命名，寻找Nonfellow有所改进，但没有考虑CSPaperRatio，找到大量不是CS领域学者，对应 Fellows VS Non-Fellows (4,420 authors / 980,931 papers)
- fellowV2: 20240522版本，使用`min(l / len(ret) * 2, 1)`计算是否是CSPaper，对应 Fellows (1,940 authors / 534,723 papers)
- fellowV3: 20240601版本，只要与CS (41008148) 及其子领域有交叉，与Astrophysics(44870925, 2097sub) 及 Astronomy(1276947, 7397sub), Planet(51244244, 268sub) 没有交叉就是CSPaper