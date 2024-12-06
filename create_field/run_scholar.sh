echo "waiting for extract to finish"

# 每隔一分钟检查一次进程是否仍在运行，输出一条消息，直到 Python 脚本结束
while pgrep -f extract_non.py > /dev/null; do
  echo "The script is still running..."
  sleep 60
done
echo "start scholar.sh"

export user=root
export password=root
export field=fellowV5
# export database=AI
export scholar=1

mkdir -p out/$field/{papers_raw,papers,links,log}

# python extract_scholar.py

python extract_abstract.py | tee out/$field/log/extract_abstract.log
python compute_key_papers_scholar.py | tee out/$field/log/compute_key_papers.log
python update_papers.py | tee out/$field/log/update_papers.log

python graph.py | tee out/$field/log/graph.log
python compute_similarity_features.py | tee out/$field/log/compute_similarity_features.log
python extract_link_features.py | tee out/$field/log/extract_link_features.log
python compute_link_prob.py | tee out/$field/log/compute_proba.log
python update_links.py | tee out/$field/log/update_links.log

python analyse_distribution.py | tee out/$field/log/analyse_distribution.log


# rsync -a --progress=info2 out/AIfellow1/{links,papers} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/AI/
# rsync -a --progress=info2 out/ACMfellowTuring/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/ACMfellowTuring/
# rsync -a --progress=info2 out/fellowTuring/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/turing/
# rsync -a --progress=info2 out/turing/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/turing/
# rsync -a --progress=info2 out/ACMfellow/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/ACMfellow/
# rsync -a --progress=info2 out/fellowVSNon/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/fellowVSNon/
# rsync -a --progress=info2 out/fellows/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/fellows/
# rsync -a --progress=info2 out/fellowV3/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/fellowV3/


# cd /home/sy/GFVis/csv/fellowV3
# cp /home/xl/GeneFlow/topicAutoSimple/output/fellowV3/{paperID2topic.json,paperIDDistribution.csv,field_leaves.csv,field_roots.csv} .
# cp -r /home/sy/MAGProcessing/create_field/out/fellowV3/{links,papers,top_field_authors.csv} .