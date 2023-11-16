echo "<<<<<<<<Running spark submit job>>>>>>>>>"

#sample command sh JdbcMysql.sh 'local' 'cluster' '1g' '0.5g' '1' "
master=$1
deployMode=$2
driverMem=$3
exec_memory=$4
exe_cores=$5
pyfile=$6
hivetb=$7
mstbl=$8
msdb=$9

spark-submit \
--master "${master}" \
--queu-name "dbcqp"
--deploy-mode "${deployMode}" \
--driver-memory "${driverMem}" \
--executor-memory "${exec_memory}" \
--executor-cores "${exe_cores}" \
"${pyfile}" "${hivetb}" "${mstbl}" "${msdb}"