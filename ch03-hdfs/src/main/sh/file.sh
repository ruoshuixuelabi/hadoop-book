# 本脚本用 Java 程序演示怎么访问 HDFS：URLCat、FileSystemCat、FileSystemDoubleCat、ListStatus、FileCopyWithProgress。
# 下面以冒号开头的几行是书稿抽取用的标记，不是 shell 语法。
: == url_cat
: == filesystem_cat
: == filesystem_double_cat
: == list_status
: == file_copy_with_progress
rm -r /Users/tom/workspace/htdg/output
export HADOOP_CLASSPATH=build/classes
: vv url_cat
# 用标准 URL 机制读取 HDFS 文件：先注册 FsUrlStreamHandlerFactory，JVM 才认得 hdfs 协议
hadoop URLCat hdfs://localhost/user/tom/quangle.txt
: ^^ url_cat
: vv filesystem_cat
# 换成 FileSystem API 打开同一个文件
hadoop FileSystemCat hdfs://localhost/user/tom/quangle.txt
: ^^ filesystem_cat
: vv filesystem_double_cat
# 用 FSDataInputStream 的 seek 定位，把同一份内容打印两遍
hadoop FileSystemDoubleCat hdfs://localhost/user/tom/quangle.txt
: ^^ filesystem_double_cat
: vv list_status
# 列出根目录与 /user/tom 这两个路径下的文件状态
hadoop ListStatus hdfs://localhost/ hdfs://localhost/user/tom
: ^^ list_status
: vv file_copy_with_progress
# 带进度提示地把本地文件复制到 HDFS
hadoop FileCopyWithProgress input/docs/1400-8.txt hdfs://localhost/user/tom/1400-8.txt
: ^^ file_copy_with_progress


