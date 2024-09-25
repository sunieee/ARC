#!/bin/bash

cd ./topicTop
minTopicSize=100

i=0
while [ $i -lt 12 ]; do
    for field in 'visualization'; do
        bash run.sh $field $minTopicSize
    done

    i=$(($i+1))
done


# cd ../topicAll

# for minTopicSize in 200 250 300; do
#     for field in 'visualization' 'CG' 'database'; do
#         bash run.sh $field $minTopicSize
#     done

#     python save_version.py
# done