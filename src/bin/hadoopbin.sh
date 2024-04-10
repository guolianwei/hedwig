export CUR_DIR="$(cd "`dirname "$0"`"; pwd)"
source ${CUR_DIR}/source_config.sh
CMD="hadoop --config ${HADOOP_CONF_DIR} $1 $2 $3 $4 $5"
# echo $CMD
$CMD


