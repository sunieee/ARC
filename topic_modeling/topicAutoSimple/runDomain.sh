
fieldpath='domain/out/graphdrawing'
# 取/后面的字符串
field=${fieldpath##*/}

cp /home/sy/MAGProcessing/${fieldpath}/papers.csv ./src/papers_${field}_autoTop.csv
rm -rf model/${field}

./geneTopics.sh ${field} 8 10
./geneOthers.sh ${field}