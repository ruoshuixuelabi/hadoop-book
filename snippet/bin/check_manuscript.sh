#!/bin/bash
# 这个脚本检查本地生成的代码清单是不是原样出现在书稿里。
# 做法是先把书稿 XML 和实际片段都做一点预处理，再交给 Python 脚本逐字比对。

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
# 先把 bin 变成绝对路径。

actual="$bin"/../actual
book_workspace=~/book-workspace/htdg-git
# 书稿工作区不在本仓库里，要单独把它的位置指出来。

for ch in ch02 ch03 ch05 ch06 ch08 ch09 ch16 ch17 ch21
# 这些章节的清单要逐章核对。
do
# 去掉程序清单上的 id 和 language 属性，并在结束标签前补一个换行，
# 这样书稿里的排版差异就不会影响后面的比对。
  # remove id and language attributes from program listings, and add a newline before </programlisting>
  sed '/<programlisting/s/ id="[^"]*"//; /<programlisting/s/ language="[^"]*"//; s|</programlisting>|\
</programlisting>|' $book_workspace/$ch.xml > /tmp/$ch.xml
  $bin/check_manuscript.py /tmp/$ch.xml $actual/$ch/*
# 真正的逐字比对交给 Python 脚本去做。
done

# 第 12 章 Avro 的清单单独走一遍，它用的 sed 规则和上面略有不同。
# Avro check
sed -e '/<programlisting/s/ id="[^"]*"//; /<programlisting/s/ language="[^"]*"//;  s|</programlisting>|\
</programlisting>|' $book_workspace/ch12.xml > /tmp/ch12.xml
$bin/check_manuscript.py /tmp/ch12.xml $actual/ch12/*

# 最后再核对一下公共模块的清单。
# Common check
$bin/check_manuscript.py /tmp/ch08-mr-types.xml $actual/common/*
