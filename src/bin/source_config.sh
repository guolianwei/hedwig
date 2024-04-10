export INSATALL_HOME="$(cd "`dirname "$0"`"/..; pwd)"
export JAVA_HOME=${INSATALL_HOME}/jdk1.8.0_401
export HADOOP_HOME=${INSATALL_HOME}/hadoop-3.3.4
export PATH=${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${PATH}
#echo "PATH=" $PATH
export HADOOP_CONF_DIR=${INSATALL_HOME}/pjconf
#java -version
kinit -kt ${HADOOP_CONF_DIR}/tempo.keytab tempo
klist -kt ${HADOOP_CONF_DIR}/tempo.keytab
export HADOOP_OPTS=-Djava.security.krb5.conf=${HADOOP_CONF_DIR}/krb5.conf

LOG(){
	echo -e "\033[36m[info]##${2}## \033[0m [`date -d today +"%Y-%m-%d %H:%M:%S"`],${1}"
}

#打印异常信息
LOG_EXCEPTIONS_INFO(){
	echo -e "\033[31m[exception]##${2}## [`date -d today +"%Y-%m-%d %H:%M:%S"`],${1}. \033[0m"
}