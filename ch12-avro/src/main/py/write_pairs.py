# 这个脚本把标准输入里的「左值,右值」文本行转换成 Avro 容器文件。
# 用法是：管道若干行文本进来，脚本把它们打包成一个 Avro 文件。
import os
import string
import sys

# 这三个模块分别负责模式、数据编解码与容器文件。
from avro import schema
from avro import io
from avro import datafile

# 只有被当作脚本直接运行时才干活，被 import 时不做任何事。
if __name__ == '__main__':
  # 命令行必须恰好给一个参数，也就是输出文件名
  if len(sys.argv) != 2:
    sys.exit('Usage: %s <data_file>' % sys.argv[0])
  # 输出文件的名字
  avro_file = sys.argv[1]
  # 以二进制方式打开输出文件
  writer = open(avro_file, 'wb')
  # 写出器：把 Python 对象按模式编码成字节
  datum_writer = io.DatumWriter()
  # 模式对象：描述一条记录里有两个字符串字段。
  # 这里用三引号把 JSON 模式原样嵌在源码里，免得再多一个模式文件。
  schema_object = schema.parse("""\
{ "type": "record",
  "name": "StringPair",
  "doc": "A pair of strings.",
  "fields": [
    {"name": "left", "type": "string"},
    {"name": "right", "type": "string"}
  ]
}""")
  # 容器文件写出器：模式会写进文件头，读的人据此还原结构
  dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
  # 一次性把标准输入读进来，逐行处理
  for line in sys.stdin.readlines():
    # 按逗号把一行拆成左右两个值；先去掉行尾的换行符
    (left, right) = string.split(line.strip(), ',')
    # 追加成一条记录
    dfw.append({'left':left, 'right':right});
  # 写完关闭
  dfw.close()