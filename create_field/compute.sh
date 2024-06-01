# while pgrep -x "python compute_key_papers.py" | tee /dev/null; do
#     sleep 1
# done

# while pgrep -x "python merge_author.py" | tee /dev/null; do
#     echo "sleep 1"
#     sleep 1
# done

export user=root
export password=root

compute() {
    field=$1
    echo "start compute $field"
    export field=$field
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
    python update_links.py | tee out/$field/log/update_links.log

    # draw distribution
    python analyse_distribution.py | tee out/$field/log/analyse_distribution.log
}


# compute VCG
# compute visualization
# compute CG
# compute AI
# compute SE
# compute HCI
# compute CN
compute CA


# rsync -a output xiaofengli@120.55.163.114:/home/xiaofengli/pyCode/GFVis/csv/ --progress=info2

# rsync -a --progress=info2 out/SE/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/SE/
# rsync -a --progress=info2 out/HCI/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/HCI/
# rsync -a --progress=info2 out/CN/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/CN/
# rsync -a --progress=info2 out/ACMfellowTuring/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/ACMfellowTuring/
# rsync -a --progress=info2 out/VCG/{links,papers,top_field_authors.csv} root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/VCG/

# rsync -a --progress=info2 root@82.156.152.182:/home/xfl/pyCode/GFVisTest/csv/AI/{links,papers,top_field_authors.csv} out/AI/