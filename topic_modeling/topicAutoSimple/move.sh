field='fellowV3'

sudo cp -r /home/xl/GeneFlow/topicAutoSimple/output/${field} ./output/${field}
sudo cp -r /home/xl/GeneFlow/topicAutoSimple/model/${field} ./model/${field}

sudo chown sy:sy -R ./output/${field}
sudo chown sy:sy -R ./model/${field}