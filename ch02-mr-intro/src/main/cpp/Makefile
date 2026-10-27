# 用 Hadoop Pipes 编译 C++ 版 MapReduce 示例。
# 编译器用 g++，因为 Pipes 需要 C++ 运行库。
CC = g++
# 这里配的是编译选项：-m32 生成 32 位代码（与 Hadoop 自带的 Pipes 库位数一致），
# -I 指向 Hadoop 自带的 c++ 头文件目录。
# 这里的 PLATFORM 是 Hadoop 发行包里的平台子目录名（如 linux-x86_32），
# 需要在命令行里用 make PLATFORM=linux-x86_32 指定。
CPPFLAGS = -m32 -I$(HADOOP_INSTALL)/c++/$(PLATFORM)/include
# 目标文件：编译 max_temperature.cpp，链接 Hadoop Pipes 库。
# 链接顺序是 -lhadooppipes -lhadooputils：前者是 Pipes 的 C++ 接口，
# 后者提供 Pipes 依赖的 Hadoop C 库。
# -Wall 打开全部警告，-g 保留调试符号，-O2 开启优化，-o $@ 指定输出名。

max_temperature: max_temperature.cpp 
	$(CC) $(CPPFLAGS) $< -Wall -L$(HADOOP_INSTALL)/c++/$(PLATFORM)/lib -lhadooppipes \
	-lhadooputils -lpthread -g -O2 -o $@
