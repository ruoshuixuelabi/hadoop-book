#!/bin/bash
# 这个脚本把各章的 Java 源码抽取成书稿里的代码清单。
set -x
# 打开命令回显，跑的时候能看到每一步实际执行了什么。

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
# 先把 bin 变成绝对路径。

actual="$bin"/../actual
# 抽取结果统一放在 snippet/actual 下面，按章节分目录。

# 先用 grep 找出带书稿标记的 Java 文件，
# 再交给 phragmite_db.pl 按标记把内容切成一个个片段。
perl $bin/phragmite_db.pl $actual/ch02/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch02-mr-intro)
perl $bin/phragmite_db.pl $actual/ch03/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch03-hdfs)
perl $bin/phragmite_db.pl $actual/ch05/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch05-io)
perl $bin/phragmite_db.pl $actual/ch12/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch12-avro)
perl $bin/phragmite_db.pl $actual/ch06/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch06-mr-dev)
# 第 6 章除了 Java 之外，还要收一个 pom.xml。
perl $bin/phragmite_db.pl $actual/ch06/ $bin/../../../hadoop-book-mr-dev/pom.xml
# 工作流配置文件也会进书稿，抽取时不做标记，整份文件算一个片段。
perl $bin/phragmite_db.pl $actual/ch06/ $bin/../../ch06-mr-dev/src/main/resources/max-temp-workflow/workflow.xml
perl $bin/phragmite_db.pl $actual/ch08/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch08-mr-types)
perl $bin/phragmite_db.pl $actual/common/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../common)
perl $bin/phragmite_db.pl $actual/ch09/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch09-mr-features)
perl $bin/phragmite_db.pl $actual/ch21/ $(grep -ElR '(// ?cc|// ?==)' --include '*.java' $bin/../../ch21-zk)
