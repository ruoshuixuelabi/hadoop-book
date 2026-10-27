#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 这个脚本检查本地生成的代码片段是不是原样出现在书稿里。
# 用法：check_manuscript.py <书稿 XML> <片段文件> [<片段文件> ...]

# Check that the expected (or actual) snippets are in the manuscript. E.g.
# bin/check_manuscript.py  ~/book-workspace/htdg-git/ch16-pig.xml expected/ch16-pig/grunt/*

import sys
# 只用到标准库 sys。

manuscript = open(sys.argv[1], 'r').read()
# 第一个参数是书稿的 XML，整个读进来。

for snippet_file in sys.argv[2:]:
# 剩下的参数都是要核对的片段文件。
  lines = open(snippet_file, 'r').readlines()
# 片段文件按行读进来。
  if lines[0].startswith("<!--"):
    doc = "".join(lines[1:]) # remove first line if a comment
# 片段文件的第一行有时是 XML 注释，比对时要把它去掉。
  else:
    doc = "".join(lines[0:])
# 没有注释就直接用全部行。
  snippet = doc.strip()
# 去掉首尾空白再找，避免换行差异造成误判。
  index = manuscript.find(snippet)
# 在书稿里找这一段文字，找得到就说明清单和书稿对得上。
  if index == -1:
    print "Snippet not found", snippet_file
# 找不到就打印是哪个文件有问题。
  #else:
  #  print "Snippet found", snippet_file

