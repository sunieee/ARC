if [ $# -lt 1 ]; then
    echo 'Wrong Format! Using `bash ./geneOthers.sh $field` to run the shell'
    
    exit 2
fi
field=$1

python words_merge.py $field
cp output/$field/topic_word_prob_merged.json output/$field/topic_word_prob_manual.json
# # 手动更改未合并的词
# # topology emotion scatterplot hmd projection learner ontology organize distortion aoi color colormap treemap hierarchy saliency collaborative dimensional holography molecular genome volume bundling scientific streamline uncertainty
python words_filter.py $field
python concat_topic.py $field
python color_topic.py $field

python cluster_topic.py $field 
python group_to_root.py $field $? 1> output/$field/topic_group.txt