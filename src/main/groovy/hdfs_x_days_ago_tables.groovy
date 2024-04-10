import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.ContentSummary
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.security.UserGroupInformation

// 设定 HDFS 的路径
def HDFS_PATH = "/user/hive/warehouse/"

// 获取 10 天前的日期
def days=2L
def ten_days_ago = java.time.LocalDate.now().minusDays(2.longValue()).toString()

// 初始化总大小
def total_size = 0
def total_finded_count = 0
def total_scaned_count = 0
def total_scaneddb_count = 0

// 设置配置文件路径
def configPath = "C:\\Users\\29267\\Desktop\\pjconf"

Configuration conf = new Configuration()

// 加载你的 Hadoop 配置文件
conf.addResource(new Path("${configPath}/core-site.xml"))
conf.addResource(new Path("${configPath}/hdfs-site.xml"))
conf.addResource(new Path("${configPath}/yarn-site.xml"))

// 设置 Kerberos 认证相关的系统属性
System.setProperty("java.security.krb5.conf", "${configPath}/krb5.conf")
System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")

UserGroupInformation.setConfiguration(conf)

// 使用你的 principal 和 keytab 文件进行 Kerberos 认证
// 请将 YOUR_PRINCIPAL 替换为你的实际 principal，将 YOUR_KEYTAB_FILE 替换为你的 keytab 文件的实际文件名
UserGroupInformation.loginUserFromKeytab("tempo", "${configPath}/tempo.keytab")

FileSystem fs = FileSystem.get(conf)



// 使用循环列出所有以 .db 结尾的目录
fs.listStatus(new Path(HDFS_PATH)).each { FileStatus dbStatus ->
    if (dbStatus.getPath().getName().endsWith(".db")) {
        total_scaneddb_count++
        fs.listStatus(dbStatus.getPath()).each { FileStatus status ->
            // 获取文件或目录的修改日期
            def modification_date = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date(status.getModificationTime()))
            total_scaned_count++
            // 检查 HDFS 路径是否为空
            if (status.getPath().toString() && modification_date) {
                println "modification_date is $modification_date table is ${status.getPath()}"

                // 如果修改日期在 10 天前之后，则打印出文件或目录的全路径和修改时间
                if (modification_date > ten_days_ago) {
                    def table = status.getPath().toString()

                    // 计算文件夹的大小（以 B 为单位）
                    def folder_size = fs.getContentSummary(status.getPath()).getLength()

                    // 更新总大小（以 MB 为单位）
                    total_size += folder_size
                    total_finded_count++
                    println "Path: $table, Modification time: $modification_date, Size: $folder_size Bytes"
                    println "Total size: $total_size Bytes ${(total_size/1024)} KB ${(total_size/1024/1024)} MB ${(total_size/1024/1024)} GB"
                    println "Total finded count: $total_finded_count tables"
                    println "Total scaned db count: $total_scaneddb_count dbs Total scaned table count: $total_scaned_count tables"
                }
            }
        }
    }
}

// 打印出总大小（以 MB 为单位）
println "Finnal Total size: $total_size Bytes ${(total_size/1024/1024)} MB  ${(total_size/1024/1024/1024)} GB"
