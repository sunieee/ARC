
field=database

# rm -rf csv/${field}
mkdir csv/${field}
cp topic/output/${field}/field_* csv/${field}
cp /home/sy/arc/post_processing/out/scigene_${field}_field/top_field_authors.csv csv/${field}
mkdir csv/${field}/papers
cp topic/paper/${field}/* csv/${field}/papers
mkdir csv/${field}/links
cp /home/sy/arc/post_processing/out/scigene_${field}_field/links/* csv/${field}/links
python reference/add_citation_context.py

cd csv
tar -zcvf ${field}.tar.gz ${field}
scp ${field}.tar.gz root@120.55.163.114:/root/tmp/
