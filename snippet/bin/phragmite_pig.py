#!/usr/bin/env python
# 这个脚本把 Pig 的运行输出按书稿标记切成一个个片段。
# 和 Hive 的版本结构一样，只是匹配的标记前缀不同。

import re
import sys
# 只用到标准库 re 和 sys。

filename = sys.argv[1]
output_dir = sys.argv[2]
# 两个参数分别是运行输出文件和片段的输出目录。

fragments = {}
active_fragments = set([])
# 当前还开着的片段集合；一行里可能同时开着多个片段，所以用集合。

for line in open(filename, 'r'):
  m = re.match(r".*--\s+vv\s+([^<]*)", line)
# 这一行表示一个片段开始，把它记成「正在收集」。
  if m:
    active_fragments.add(m.group(1).strip())
# 片段的名字就是标记里写的那个名字。
    continue
  m = re.match(r".*--\s+\^\^\s+([^<]*)", line)
  if m:
    active_fragments.remove(m.group(1).strip())
# 这一行表示片段结束，从「正在收集」里去掉。
    continue
  for fragment in active_fragments:
   fragments[fragment] = fragments.get(fragment, '') + line
# 正在收集的片段都把这一行追加进去。
   
for fragment in fragments:
# 每个片段写成一个 XML 文件。
  file = open(output_dir + "/" + fragment + ".xml", 'w')
# 文件名就是片段名。
  file.write('<screen format="linespecific">' + fragments[fragment].strip() + "</screen>" )
# 书稿里用 screen 元素把这段内容包起来。
