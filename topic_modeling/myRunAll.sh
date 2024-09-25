#!/bin/bash
cd ./topicAll

minTopicSize=200

i=0
while [ $i -lt 1 ]; do
    for field in 'fellow'; do
        bash run.sh $field $minTopicSize
    done

    python save_version.py $field
    i=$(($i+1))
done