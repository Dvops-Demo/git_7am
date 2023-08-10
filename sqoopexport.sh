====== While exporting hive table to mysql. We have to make sure that mysql table is avaiable ================

sqoop export --connect \
jdbc:mysql://localhost/abc \
--table msdept \
--username root --password root \
--export-dir /user/hive/warehouse/mydb.db/dept \
--m 1 \
-- driver com.mysql.jdbc.Driver \
--input-fields-terminated-by ','