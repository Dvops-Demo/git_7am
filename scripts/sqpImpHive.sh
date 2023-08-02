export conn=$jdbc
rdbTbl=$1

sqoop import --connect conn --username=root --password=root --table rdbTbl --fields-terminated-by '|' 
--lines-terminated-by '\n' --hive-import --create-hive-table --hive-table empnew --outdir sqoop_import_project;
