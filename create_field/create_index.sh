#!/bin/bash

# 定义数据库连接信息
DB_USER="root"
DB_PASS="root"
DB_NAME="MAG_OA"

# 执行每个索引创建，并打印进度
echo "开始创建索引..."

mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D "openalex" -e "CREATE INDEX cited_index ON paper_reference_no_duplicate (citedpaperID);" && echo "索引 cited_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX citing_cited_index ON CitationContextContent (citingpaperID, citedpaperID);" && echo "索引 citing_cited_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX name_index ON authors (name);" && echo "索引 name_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX children_index ON field_children (parentID);" && echo "索引 children_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX author_index ON paper_author (authorID);" && echo "索引 author_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX title_index ON papers (title);" && echo "索引 title_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX field_index ON papers_field (fieldID);" && echo "索引 field_index 创建完成" &


# wait
# 不需要手动分成两组，如果表锁定，后续创建索引的进程会等待

mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D "openalex" -e "CREATE INDEX citing_index ON paper_reference_no_duplicate (citingpaperID);" && echo "索引 citing_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX parent_index ON field_children (childrenID);" && echo "索引 parent_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX paper_index ON paper_author (paperID);" && echo "索引 paper_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX conference_index ON papers (ConferenceID);" && echo "索引 conference_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX journal_index ON papers (JournalID);" && echo "索引 journal_index 创建完成" &
mysql -h192.168.0.140 -u $DB_USER -p$DB_PASS -D $DB_NAME -e "CREATE INDEX paper_index ON papers_field (paperID);" && echo "索引 paper_index 创建完成" &


# 等待所有后台进程完成
wait

echo "所有索引创建完成！"