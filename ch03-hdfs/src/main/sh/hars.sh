# 本脚本演示 HDFS 归档（HAR）的完整生命周期：创建、检查、列出与删除。
# 下面以冒号开头的几行是书稿抽取用的标记，不是 shell 语法。
: == har_ls_files
: == har_create
: == har_inspect
: == har_ls
: == har_ls_long
: == har_rmr
rsync -avz --exclude '.svn' /Users/tom/workspace/htdg/input/fileinput/ /tmp/fileinput
hadoop fs -copyFromLocal /tmp/fileinput /my/files
rm -rf /tmp/fileinput
: vv har_ls_files
# 递归列出 /my/files 下的全部文件，这是打包之前的原始状态
hadoop fs -lsr /my/files
: ^^ har_ls_files
: vv har_create
# 把 /my/files 打包成 /my/files.har
hadoop archive -archiveName files.har /my/files /my
: ^^ har_create
: vv har_inspect
# 查看归档文件本身，以及归档里保存下来的原始目录
hadoop fs -ls /my
hadoop fs -ls /my/files.har
: ^^ har_inspect
: vv har_ls
# 通过 har 协议列出归档内部的内容
hadoop fs -lsr har:///my/files.har
: ^^ har_ls
: vv har_ls_long
# 用完整路径深入到归档内部的子目录，第二条还显式写出了 NameNode 的地址
hadoop fs -lsr har:///my/files.har/my/files/dir
hadoop fs -lsr har://hdfs-localhost:8020/my/files.har/my/files/dir
: ^^ har_ls_long
: vv har_rmr
# 先删归档文件，再删原始目录
hadoop fs -rmr /my/files.har
: ^^ har_rmr
hadoop fs -rmr /my/files