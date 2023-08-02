#export jdbc="jdbc:mysql://localhost:3306/hiveimport" this was configured in edge node
export conn=$jdbc

rdbTbl=$1

echo "mysql table name is "${rdbTbl}""


sqoop import --connect \
"${conn}" \
--username root --password root \
--table "${rdbTbl}" \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--hive-import --hive-overwrite --hive-table newcars \
--m 1