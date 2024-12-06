export user=root
export password=root
export database=MAG_OA
export field=visualization

echo "==========prepare $field=========="
# rm -rf out/$database
mkdir -p out/$field/{csv,log}

python match_conference_journal.py | tee out/$field/log/match_conference_journal.log
# 手动检查是否成功，并将结果保存到`conference_journal_modify.csv`
cp out/$field/journal_conference.csv out/$field/journal_conference_modify.csv
python extract_paperID.py | tee out/$field/log/extract_paperID.log
python extract_scigene_field.py | tee out/$field/log/extract_scigene_field.log
# 对于较大的field（如AI）分批处理，然后合并，AI.yaml 分开AI1 + AI2 + AI3 + AI4
# python merge_scigene_field.py | tee out/$field/log/merge_scigene_field.log
python extract_abstract.py | tee out/$field/log/extract_abstract.log
python match_author.py | tee out/$field/log/match_author.log
python filter_match.py | tee out/$field/log/filter_match.log
# 可选：最后一个脚本在手动筛选`out/{field}/match_modify.csv`之后再运行
python merge_author.py | tee out/$field/log/merge.log


echo "==========start compute $field=========="
# rm -rf out/$field/{papers_raw,papers,links,log}
mkdir -p out/$field/{papers_raw,papers,links}

# # compute node
python create_mappings.py
python compute_key_papers.py | tee out/$field/log/compute_key_papers.log
python update_papers.py | tee out/$field/log/update_papers.log

# # compute edge
python graph.py | tee out/$field/log/graph.log
python compute_similarity_features.py | tee out/$field/log/compute_similarity_features.log
python extract_link_features.py | tee out/$field/log/extract_link_features.log
python compute_link_prob.py | tee out/$field/log/compute_proba.log
# 注意citation context仍然选择MACG数据库
python update_links.py | tee out/$field/log/update_links.log
python analyse_distribution.py | tee out/$field/log/analyse_distribution.log

rsync -a --progress=info2 out/$field/{links,papers,top_field_authors.csv} root@ye-sun.com:/root/pyCode/v2_MAG_OA/csv/$field/
# rsync -a --progress=info2 root@ye-sun.com:/home/xfl/pyCode/GFVisTest/csv/AI/{links,papers,top_field_authors.csv} out/AI/