
export user=root
export password=root
export field=graphdrawing

mkdir -p out/$field/{papers_raw,papers,links,log}

# python extract_papers.py
# python update_papers.py

python graph.py
python compute_similarity_features.py 
python extract_link_features.py
python compute_link_prob.py
python update_links.py

python analyse_distribution.py

mkdir -p /home/sy/GFVis/csv/domain/$field/
cp out/$field/{papers.csv,links.csv,paperIDDistribution.csv,field_leaves.csv} /home/sy/GFVis/csv/domain/$field/