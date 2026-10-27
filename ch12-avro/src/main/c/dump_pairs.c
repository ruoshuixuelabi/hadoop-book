#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

// 这个小程序用 Avro 的 C 库把一个 Avro 容器文件里的记录逐条读出来，
// 并按「左值,右值」的形式打印到标准输出，方便人眼检查文件内容。
int main(int argc, char *argv[]) {
  // 命令行必须恰好给一个参数，也就是要读取的 Avro 文件名。
  if (argc != 2) {
    fprintf(stderr, "Usage: dump_pairs <data_file>\n");
    exit(EXIT_FAILURE);
  }
  
  // 要读取的文件名
  const char *avrofile = argv[1];
  // 解析模式失败时 Avro 会把详细原因写进这个结构体
  avro_schema_error_t error;
  // 打开文件后拿到的读取器，整个读取过程都靠它
  avro_file_reader_t filereader;
  // 一条记录的通用容器
  avro_datum_t pair;
  // 从记录里取出来的单个字段
  avro_datum_t left;
  avro_datum_t right;
  // 读取操作的返回值：0 表示成功
  int rval;
  // 字符串字段取出来之后指向 Avro 内部缓冲区的指针
  char *p;

  // 打开 Avro 文件并读取它的模式；这个调用的返回值应当检查，
  // 原始示例里没有检查，文件不存在或格式不对时后续操作会出问题。
  avro_file_reader(avrofile, &filereader);
  // 逐条读取记录，直到读完为止
  while (1) {
    // 读取下一条记录；返回非 0 表示已经到文件末尾
    rval = avro_file_reader_read(filereader, NULL, &pair);
    if (rval) break;
    // 按字段名取 left；取到就打印，打印时用逗号分隔
    if (avro_record_get(pair, "left", &left) == 0) {
      // 取出字符串内容，它仍指向 Avro 自己的缓冲区，不能释放
      avro_string_get(left, &p);
      fprintf(stdout, "%s,", p);
    }
    // 按字段名取 right；取到就打印，换行结束这一条记录
    if (avro_record_get(pair, "right", &right) == 0) {
      avro_string_get(right, &p);
      fprintf(stdout, "%s\n", p);
    }
  }
  // 关闭读取器，释放它占用的资源
  avro_file_reader_close(filereader);
  return 0;
}