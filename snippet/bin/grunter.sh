#!/bin/bash
# 这个脚本在本地跑第 16 章的每个 grunt 脚本，并把输出整理成书稿清单。

set -x
set -e
# 任何一步失败就立刻退出，避免拿半截输出继续往下走。

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

actual="$bin"/../actual
# 抽取出来的清单放在 snippet/actual/ch16 下面。

cd "$bin"/../..
# 切到仓库根目录，下面就能用相对路径找到 ch16-pig。

rm -rf $actual/ch16
# 先把上一轮的旧结果清掉。
mkdir -p $actual/ch16
# 再建一个干净的输出目录。

for f in ch16-pig/src/main/grunt/*.grunt; do
# 每个 grunt 脚本单独跑一遍。
  out=$f.output.txt
# 中间结果先写在一个临时文件里。
  pig -x local < $f 2> /dev/null \
      | grep -v INFO \
      | grep -v '^grunt> $' \
      | sed -e 's|&|\&amp;|g' \
            -e 's|"|\&quot;|g' \
            -e 's|>|\&gt;|g' \
            -e 's|<|\&lt;|g' \
            -e 's|^\(grunt&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
            -e 's|^\(&gt;&gt; \)\(.*\)|<prompt moreinfo="none">\1</prompt><userinput moreinfo="none">\2</userinput>|' \
      > $out
# 这一串管道做三件事：滤掉日志信息、把特殊字符转义成 XML 实体、
# 再把命令行提示符换成书稿里用的标签。
  cat $out
# 把中间结果打出来，方便肉眼核对。
  python "$bin"/phragmite_pig.py $out $actual/ch16
# 再由抽取脚本按书稿标记把内容切成一个个片段。
  rm $out
# 临时文件用完就删。
done

