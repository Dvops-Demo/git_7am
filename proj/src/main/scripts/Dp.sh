mysql=$1
hivetbl=$2
conntn=$3
#export conn=jdbc:mysql://localhost:3306/abc
#sqoop import --connect jdbc:mysql://localhost:3306/abc --username root --password root --table emp --hive-import --hive-overwrite --hive-table hiveimptbl -m 1

sqoop import --connect \
"${conntn}" \
--username root --password root \
--table "${mysql}" \
--fields-terminated-by "|" \
--lines-terminated-by "\n" \
--hive-import --hive-overwrite --hive-table "${hivetbl}" \
--m 1