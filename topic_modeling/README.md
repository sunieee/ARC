

以下工作均在`./topicAutoSimple`目录下运行

1. 拷贝文件csv到src文件夹下，命名规范为 `papers_{field}_autoTop.csv`，确保有paperID, title, abstract列，且paperID, title列不能有元素为nan
2. 以下步骤从simple.py中截取，仅含运行文件和运行结果
	1. 运行命令 `bash geneTopics.sh {field} {minTopicSize} {lowerTopicCount}`生成话题；其中minTopicSize为bertopic参数，至少多少篇文章作一个话题；lowerTopicCount表示希望至少学到多少个话题
	2. 运行命令 `bash geneOthers.sh {field}` 生成话题tree等信息。
	