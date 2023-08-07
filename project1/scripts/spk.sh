echo "<<<<<<<<Running spark submit job>>>>>>>>>"

#sample command sh spk.sh 'local' 'client' '1g' '0.5g' '1' "
master=$1
deployMode=$2
driverMem=$3
exec_memory=$4
exe_cores=$5

spark-submit \
--master "${master}" \
--deploy-mode "${deployMode}" \
--driver-memory "${driverMem}" \
--executor-memory "${exec_memory}" \
--executor-cores "${exe_cores}" \
etl.py
