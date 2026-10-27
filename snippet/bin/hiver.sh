#!/bin/bash
# 这个脚本在本地跑第 17 章的每个 hive 脚本，并把输出整理成书稿清单。
#
# Usage:
# hiver.sh
# hiver.sh -hiveconf fs.defaultFS=file:/// -hiveconf mapreduce.framework.name=local

# 脚本后面的参数会原样传给 hive 命令，用来指定 Hive 的配置。
set -x
set -e
# 任何一步失败就立刻退出。

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual
# 抽取出来的清单放在 snippet/actual/ch17 下面。

cd "$bin"/../..
# 切到仓库根目录，下面就能用相对路径找到 ch17-hive。

# 清理旧结果的那句被注释掉了，所以输出目录里可能还留着上一轮的文件。
#rm -rf $actual/ch17
mkdir -p $actual/ch17
# 输出目录不存在就建一个。

for f in ch17-hive/src/main/hive/*.hive; do
# 每个 hive 脚本单独跑一遍。
  out=$f.output.txt
# 中间结果先写在一个临时文件里。
  hive "$@" -S < $f 2> /dev/null \
    | grep -v '^hive> $' \
    | grep -v '^$' \
    | sed -e 's|&|\&amp;|g' \
          -e 's|"|\&quot;|g' \
          -e 's|>|\&gt;|g' \
          -e 's|<|\&lt;|g' \
          -e 's|	|    |g' \
          -e 's|^\(hive&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
          -e 's|^\(    &gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
          -e 's|Hank    |Hank   |g' \
          -e 's|0    NULL    NULL|0    NULL NULL|' \
          -e 's|NULL    NULL    |NULL   NULL |' \
    > $out
# 这一串管道滤掉空行和提示符，把特殊字符转义成 XML 实体，
# 再把命令行提示符换成书稿里用的标签。
   
  cat $out
# 把中间结果打出来，方便肉眼核对。
  python "$bin"/phragmite_hive.py $out $actual/ch17
# 再由抽取脚本按书稿标记把内容切成一个个片段。
  rm $out
# 临时文件用完就删。
done

