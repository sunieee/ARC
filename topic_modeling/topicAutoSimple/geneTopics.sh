# 生成某一领域topic分布和paper-topic对应关系的脚本

export TRANSFORMERS_CACHE=~/.cache/torch
export http_proxy=http://127.0.0.1:7890/
export https_proxy=http://127.0.0.1:7890/
export model_path='/home/sy/.cache/torch/sentence_transformers/sentence-transformers_paraphrase-MiniLM-L12-v2'


if [ $# -lt 3 ]; then
    echo 'Wrong Format! Using `bash ./geneTopics.sh $field $minTopicSie $minTopicCount` to run the shell'
    
    exit 2
fi

field=$1
minTopicSize=$2
minTopicCount=$3

mkdir -p output/$field

python bertopic_title_abstract.py $field autoTop $minTopicSize $minTopicCount > output/$field/bertopic.txt

if [ $? -ne 0 ]; then
    echo 'Error in bertopic_title_abstract.py'
    exit -2
fi

