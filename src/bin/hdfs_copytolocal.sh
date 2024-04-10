#!/bin/bash
export CUR_DIR="$(cd "`dirname "$0"`"; pwd)"
source ${CUR_DIR}/source_config.sh

# 设定HDFS的路径
HDFS_PATH=$1

# 检查HDFS路径是否为空
if [ -z "$HDFS_PATH" ]; then
    echo "参数错误：HDFS路径不能为空"
    exit 1
fi

# 检查HDFS路径是否以"/user/hive/warehouse"开头
if [[ $HDFS_PATH != /user/hive/warehouse* ]]; then
    echo "参数错误：HDFS路径必须以'/user/hive/warehouse'开头"
    exit 1
fi

# 设定本地的路径
LOCAL_PATH=${INSATALL_HOME}/hivedata
# 对应数据表的文件夹
FILE_LOCAL_PATH=${LOCAL_PATH}${HDFS_PATH}
mkdir -p $FILE_LOCAL_PATH

# 获取HDFS目录中的所有文件
FILE_LIST=$(hadoop --config ${HADOOP_CONF_DIR} fs -ls $HDFS_PATH | awk '{print $8}')
start_time=$(date +%s)
copied_files=0
copied_size=0

# 使用循环来拷贝每个以"part-"开头的文件
for file in $FILE_LIST; do
    filename=$(basename $file)
    if [[ $filename == part-* ]]; then
        # 开始计时
        start_time_file=$(date +%s)
        echo "$file  TO >>  $LOCAL_PATH/$filename"
        filedir=$(dirname $file)
        
        CMD="hadoop fs -copyToLocal -f $file ${FILE_LOCAL_PATH}/${filename}"
        echo $CMD
        $CMD
        # 结束计时并显示结果
        end_time_file=$(date +%s)
        echo "File $filename copied in $(($end_time_file-$start_time_file)) seconds"
        # 更新已拷贝的文件数和大小
        copied_files=$((copied_files+1))
        copied_size=$((copied_size+$(hadoop fs -du -s $file | awk '{print $1}')))
    fi
done

# 结束计时并显示结果
end_time=$(date +%s)
echo "Total copy time: $(($end_time-$start_time)) seconds"

echo "Total copied files: $copied_files"
echo "Total copied size: $copied_size bytes"

local_size=$(du -sb ${FILE_LOCAL_PATH} | awk '{print $1}')

# 检查本地文件的大小是否和已拷贝的大小相同
if [ $local_size -ne $copied_size ]; then
    echo "警告：本地文件的总大小（$local_size bytes）和已拷贝的大小（$copied_size bytes）不一致！"
fi

# 计算并显示文件拷贝的平均速率
average_speed=$(echo "scale=2; $copied_size / 1024 / 1024 / $(expr $end_time - $start_time)" | bc)
echo "Average copy speed: $average_speed MB/s"


