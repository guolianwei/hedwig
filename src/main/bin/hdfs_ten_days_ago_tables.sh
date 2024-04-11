#!/bin/bash
export CUR_DIR="$(cd "`dirname "$0"`"; pwd)"
source ${CUR_DIR}/source_config.sh
# 定义一个函数来处理SIGINT信号
handle_sigint() {
    echo "Received Ctrl+C. Exiting..."
    exit 1
}
#!/bin/bash

# 设定HDFS的路径
HDFS_PATH="/user/hive/warehouse/"

# 获取2天前的日期
ten_days_ago=$(date -d '2 days ago' +%Y-%m-%d)

# 初始化总大小
total_size=0
total_count=0
trap 'handle_sigint' SIGINT
# 使用循环列出所有以.db结尾的目录
for db in $(hadoop fs -ls $HDFS_PATH | awk '/\.db$/ {print $8}'); do
    hadoop fs -ls $db | awk '{printf "%s %s\n", $6,$8}' > output.txt
    # 列出数据库目录下的所有文件和目录
    while read line; do
        # 获取文件或目录的修改日期
        modification_date=$(echo $line | awk '{print $1}')
        table=$(echo $line | awk '{print $2}')
        # 检查HDFS路径是否为空
        if [ -z "$table" ]; then
           continue
        fi

        if [ -z "$modification_date" ]; then
           continue
        fi

        LOG "modification_date is $modification_date table is $table"
        # 如果修改日期在10天前之后，则打印出文件或目录的全路径和修改时间
        if [[ "$modification_date" > "$ten_days_ago" ]]; then
            # 获取文件或目录的全路径
            table=$(echo $line | awk '{print $2}')

            # 计算文件夹的大小（以B为单位）
            folder_size=$(hadoop fs -du -s $table | awk '{print $1}')

            # 更新总大小（以MB为单位）
            total_size=$((total_size+folder_size))
            total_count=$((total_count+1))
            LOG "Path: $table, Modification time: $modification_date, Size: $folder_size Bytes"
            LOG "Total size: $total_size Bytes $((total_size/1024/1024)) MB"
            LOG "Total count: $total_count tables"
        fi
    done < output.txt
done

# 打印出总大小（以MB为单位）
LOG "Finnal Total size: $total_size Bytes $((total_size/1024/1024)) MB"