#include <algorithm>
#include <limits>
#include <stdint.h>
#include <string>

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"

// 这个 Mapper 的职责是逐条处理输入记录，抽出年份与气温并滤掉无效读数。
class MaxTemperatureMapper : public HadoopPipes::Mapper {
public:
  MaxTemperatureMapper(HadoopPipes::TaskContext& context) {
  }
  // 处理单条输入记录，把「年份, 气温」作为键值对发射出去。
  void map(HadoopPipes::MapContext& context) {
    std::string line = context.getInputValue();
    // 定长文本格式的 NCDC 记录：年份在第 16 至 19 列，气温在第 88 至 92 列，质量位在第 93 列。
    // 这里是 0 起始的下标，所以要各减 1。
    std::string year = line.substr(15, 4);
    std::string airTemperature = line.substr(87, 5);
    std::string q = line.substr(92, 1);
    // 加号加四个 9 是「缺测」的哨兵值；质量位只认 0、1、4、5、9 这几种有效读数
    if (airTemperature != "+9999" &&
        (q == "0" || q == "1" || q == "4" || q == "5" || q == "9")) {
      context.emit(year, airTemperature);
    }
  }
};

// 这个 Reducer 的职责是把同一个年份下的多个气温归约成一个最大值。
class MapTemperatureReducer : public HadoopPipes::Reducer {
public:
  MapTemperatureReducer(HadoopPipes::TaskContext& context) {
  }
  // 遍历当前键的所有取值，求出最大值后连同键一起发射。
  void reduce(HadoopPipes::ReduceContext& context) {
    int maxValue = INT_MIN;
    while (context.nextValue()) {
      maxValue = std::max(maxValue, HadoopUtils::toInt(context.getInputValue()));
    }
    context.emit(context.getInputKey(), HadoopUtils::toString(maxValue));
  }
};

// 程序入口：把 Mapper 与 Reducer 交给 Hadoop Pipes 的模板工厂，由它统一驱动任务。
int main(int argc, char *argv[]) {
  return HadoopPipes::runTask(HadoopPipes::TemplateFactory<MaxTemperatureMapper, 
                              MapTemperatureReducer>());
}