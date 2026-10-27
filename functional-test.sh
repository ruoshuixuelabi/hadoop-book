# 端到端功能测试脚本：在当前目录下载一份完整发行版 Hadoop，再用它编译并验证全书示例
WORKSPACE=$(pwd)
# 选用 CDH 打包的 Hadoop 发行版而不是 Apache 官方版：本书示例是按这个版本验证的，换版本行为可能对不上
TARBALL_URL=http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.6.0-cdh5.13.0.tar.gz
# 版本号必须与上面压缩包里的版本严格一致，它会覆盖各模块 pom.xml 里默认的 Hadoop 版本
HADOOP_VERSION=2.6.0-cdh5.13.0

# 直接把下载流传给 tar 解压，磁盘上不会留下压缩包
curl $TARBALL_URL | tar zxf -
# 导出的变量会被下面的 Maven 构建继承，示例代码靠它找到刚解压出来的 Hadoop 命令与配置
export HADOOP_HOME=$WORKSPACE/$(basename $TARBALL_URL .tar.gz)
# 这里用 verify 而不是 package，它会依次跑单元测试与集成测试；-DargLine 把工作目录传进 JVM，集成测试靠它定位各章的示例目录
mvn clean verify -Dhadoop.version=$HADOOP_VERSION -DargLine="-Dhadoop.book.basedir=$WORKSPACE"