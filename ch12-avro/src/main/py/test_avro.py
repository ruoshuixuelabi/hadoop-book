# 这个测试文件演示 Avro 的 Python 客户端怎么用。
# 它依赖的是老版本的 avro Python 包（模块名是 avro.schema / avro.io / avro.datafile），
# 三个模块分别负责「模式」「数据值的编解码」「容器文件」这三件事。
import os
import unittest
from avro import schema
from avro import io
from avro import datafile

# 下面这组用例覆盖 Avro Python 客户端最基本的两件事：
# 一是往容器文件里写数据再读回来，二是按一份模式写出多条记录。
class TestAvro(unittest.TestCase):

  # 第一个用例：写一条记录进容器文件，再读回来比对。
  def test_container(self):
    # 以二进制方式打开输出文件：Avro 的容器文件是二进制格式，不能用文本模式
    writer = open('data.avro', 'wb')
    # 写出器：负责把 Python 对象按模式编码成字节
    datum_writer = io.DatumWriter()
    # 模式对象：用 Avro 的 JSON 语法描述「一条记录里有两个字符串字段」
    schema_object = schema.parse("""\
{ "type": "record",
  "name": "StringPair",
  "doc": "A pair of strings.",
  "fields": [
    {"name": "left", "type": "string"},
    {"name": "right", "type": "string"}
  ]
}
    """)
    # 把模式交给容器文件写出器：容器文件头里会记下这份模式，
    # 所以读的人不必另外知道数据的结构
    dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
    # 准备一条记录，键就是模式里的字段名
    datum = {'left':'L', 'right':'R'}
    # 追加这一条记录
    dfw.append(datum)
    # 关闭写出器：收尾工作（写文件尾、刷盘）都在这一步
    dfw.close()
    
    # 以二进制方式打开同一个文件来读
    reader = open('data.avro', 'rb')
    # 读入器：负责把字节按模式还原成 Python 对象
    datum_reader = io.DatumReader()
    # 容器文件读取器：它本身就是个迭代器，可以直接 for
    dfr = datafile.DataFileReader(reader, datum_reader)
    # 把读到的记录收集到列表里，方便后面断言
    data = []
    for datum in dfr:
      data.append(datum)
      
    # 文件里应当正好有一条记录
    self.assertEquals(1, len(data));
    # 而且这条记录的内容与写进去的一致
    self.assertEquals(datum, data[0]);
    
  # 第二个用例：按一份模式写出多条记录。
  def test_write_data(self):
    # 输出文件
    writer = open('pairs.avro', 'wb')
    # 写出器
    datum_writer = io.DatumWriter()
    # 模式直接从一个 .avsc 文件里读；
    # 注意这里的路径是原作者机器上的绝对路径，换机器要改成自己的路径
    schema_object = schema.parse(open('/Users/tom/workspace/hadoop-book-avro/src/main/java/Pair.avsc').read())
    # 容器文件写出器
    dfw = datafile.DataFileWriter(writer, datum_writer, schema_object)
    # 连续追加四条记录，每条两个字段
    dfw.append({'left':'a', 'right':'1'})
    dfw.append({'left':'c', 'right':'2'})
    dfw.append({'left':'b', 'right':'3'})
    dfw.append({'left':'b', 'right':'2'})
    # 写完关闭
    dfw.close()
    
# 作为脚本直接运行时，交给 unittest 去发现并执行上面那些用例
if __name__ == '__main__':
  unittest.main()
