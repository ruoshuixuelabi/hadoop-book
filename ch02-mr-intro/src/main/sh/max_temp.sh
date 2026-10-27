# 本脚本把「求最高气温」这个例子用三种语言各跑一遍：Java（Hadoop Pipes）、Ruby、Python。
# 下面以冒号开头的几行是书稿抽取用的标记，不是 shell 语法。
: == max_temp_java
: == max_temp_java_output
: == max_temp_ruby_map
: == max_temp_ruby_pipeline
: == max_temp_python_pipeline
rm -r /Users/tom/workspace/htdg/output
: vv max_temp_java
# 用 Hadoop Pipes 的 C++ 程序求最高气温
export HADOOP_CLASSPATH=build/classes
hadoop MaxTemperature input/ncdc/sample.txt output
: ^^ max_temp_java
: vv max_temp_java_output
# 查看归约之后的结果文件
cat output/part-00000
: ^^ max_temp_java_output
: vv max_temp_ruby_map
# 用 Ruby 版的 Mapper 处理同一份输入
cat input/ncdc/sample.txt | ch02-mr-intro/src/main/ruby/max_temperature_map.rb
: ^^ max_temp_ruby_map
: vv max_temp_ruby_pipeline
# 用 Ruby 版的 Mapper 接 sort 再接 Reduce，拼出一条完整的流水线
cat input/ncdc/sample.txt | \
  ch02-mr-intro/src/main/ruby/max_temperature_map.rb | \
  sort | ch02-mr-intro/src/main/ruby/max_temperature_reduce.rb
: ^^ max_temp_ruby_pipeline
: vv max_temp_python_pipeline
# 用 Python 版的 Mapper 接 sort 再接 Reduce，与 Ruby 版对照着看
cat input/ncdc/sample.txt | \
  ch02-mr-intro/src/main/python/max_temperature_map.py | \
  sort | ch02-mr-intro/src/main/python/max_temperature_reduce.py
: ^^ max_temp_python_pipeline
  