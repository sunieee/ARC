
export user=root
export password=root
export field=graphvis

mkdir -p out/$field/{papers_raw,papers,links,log}

# python extract_papers.py
# python update_papers.py

python graph.py
python compute_similarity_features.py 
python extract_link_features.py
python compute_link_prob.py
python update_links.py

python analyse_distribution.py