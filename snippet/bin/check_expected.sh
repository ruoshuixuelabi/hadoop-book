#!/bin/bash
# 这个脚本把「书里印出来的」代码片段和「本地跑出来的」代码片段逐个对比。
# 两边的目录结构是一样的：expected 下是期望结果，actual 下是这次的实际结果。

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
# 先把 bin 变成绝对路径，这样从任何目录调用都能找到脚本。

actual="$bin"/../actual
expected="$bin"/../expected
# 这里的 expected 是书稿里的期望结果，actual 是本次实际跑出来的结果。

for f in $expected/ch16-pig/grunt/*.xml; do
# 逐个遍历第 16 章 grunt 脚本生成的书稿片段。
  echo $f
  f_actual=$actual/ch16-pig/grunt/`basename $f`
# 期望文件和实际文件一一对应，只是所在目录不同、文件名相同。
  diff $f $f_actual > /dev/null
# 先静默比一次，这里只关心有没有差异。
  if [ $? != 0 ]; then
    echo "Expected file $f different to actual $f_actual:"
# 有差异就说明期望文件和实际文件名是对得上的，只是内容不同。
    diff $f $f_actual
# 把两边的差异完整打出来，方便人工判断。
# 紧接着那句退出命令是被注释掉的，所以发现差异脚本也不会失败。
    #exit 1
  fi
done