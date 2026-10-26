package crunch;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;

import java.io.Serializable;
import java.util.Collections;
import org.apache.crunch.io.To;

import static org.apache.crunch.types.avro.Avros.*;

// Crunch version of ch12-avro AvroGenericMaxTemperature
/**
 * 用 Crunch 求每年的最高气温，结果写成 Avro 文件
 *
 * <p>这是第 12 章 {@code AvroGenericMaxTemperature} 的 Crunch 版本：
 * 计算过程和普通版本一样，
 * 区别是中间数据和最终输出都用 Avro 的模式来描述
 * <p>这里用的是 Avro 的「通用模式」（{@code GenericData.Record}）：
 * 字段按名字存取，不需要为每个字段生成专门的类
 */
public class AvroGenericMaxTemperatureCrunch implements Serializable {

  // 输出用的 Avro 模式：年、气温、站号三个字段
  // 把模式写死在这里，示例里省事；真实项目通常单独放一个 .avsc 文件
  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{" +
          "  \"type\": \"record\"," +
          "  \"name\": \"WeatherRecord\"," +
          "  \"doc\": \"A weather reading.\"," +
          "  \"fields\": [" +
          "    {\"name\": \"year\", \"type\": \"int\"}," +
          "    {\"name\": \"temperature\", \"type\": \"int\"}," +
          "    {\"name\": \"stationId\", \"type\": \"string\"}" +
          "  ]" +
          "}"
  );

  /**
   * 作业入口
   *
   * @param args 两个参数：输入目录、输出目录
   * @throws Exception 作业执行失败
   */
  public static void main(String[] args) throws Exception {
    // 参数必须是两个
    if (args.length != 2) {
      System.err.println("Usage: AvroGenericMaxTemperatureCrunch <input> <output>");
      // 直接退出
      System.exit(-1);
    }

    // 建立管道
    Pipeline pipeline = new MRPipeline(AvroGenericMaxTemperatureCrunch.class);
    // 读入文本文件
    PCollection<String> records = pipeline.readTextFile(args[0]);
    
    // 用自定义的组合子求最大值
    // Crunch 内置的组合子只认基本类型，这里要挑出温度最高的那条记录
    PTable<Integer, GenericData.Record> maxTemps = records
      .parallelDo(toYearRecordPairsFn(), tableOf(ints(), generics(SCHEMA)))
      .groupByKey()
      .combineValues(new Aggregators.SimpleAggregator<GenericData.Record>() {
        // 当前温度最高的记录
        // 用 transient 是因为 Avro 记录本身不可序列化，不能作为字段序列化
        transient GenericData.Record max;
        /**
         * 开始处理新的一组数据
         */
        @Override
        public void reset() {
          // 先清空，等第一条数据进来
          max = null;
        }

        /**
         * 处理一条记录，更新当前最大值
         *
         * @param value 一条天气记录
         */
        @Override
        public void update(GenericData.Record value) {
          // 还没有记录，或者这一条更热
          if (max == null ||
              (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
            // 复制一份存起来，不能直接存引用
            max = newWeatherRecord(value);
          }
        }

        /**
         * 按模式复制一条记录
         *
         * @param value 要复制的记录
         * @return 复制出来的新记录
         */
        private GenericData.Record newWeatherRecord(GenericData.Record value) {
          // 按同一个模式建一条空记录
          GenericData.Record record = new GenericData.Record(SCHEMA);
          // 逐个字段搬过去
          record.put("year", value.get("year"));
          // 温度
          record.put("temperature", value.get("temperature"));
          // 站号
          record.put("stationId", value.get("stationId"));
          // 返回复制结果
          return record;
        }

        /**
         * 给出这一组的结果
         *
         * @return 只有一条记录的可迭代对象
         */
        @Override
        public Iterable<GenericData.Record> results() {
          // 一组只有一个最大值，用 singleton 包一下就够了
          return Collections.singleton(max);
        }
      });
    
    // 结果按 Avro 格式写出
    pipeline.write(maxTemps, To.avroFile(args[1]));
    // 触发作业执行
    pipeline.run();
  }

  /**
   * 造一个把文本行变成「年 -> Avro 记录」键值对的 {@code DoFn}
   *
   * @return 供 {@code parallelDo} 使用的处理函数
   */
  private static DoFn<String, Pair<Integer, GenericData.Record>> toYearRecordPairsFn() {
    return new DoFn<String, Pair<Integer, GenericData.Record>>() {
      // 解析器复用
      private NcdcRecordParser parser = new NcdcRecordParser();
      // 复用的输出记录对象
      // 不可序列化，所以声明成 transient
      private transient GenericData.Record record;
      /**
       * 处理一行 NCDC 记录
       *
       * @param input 一行文本
       * @param emitter 用来发出结果
       */
      @Override
      public void process(String input, Emitter<Pair<Integer, GenericData.Record>> emitter) {
        // 拆成各个字段
        parser.parse(input.toString());
        // 只要温度合格的记录
        if (parser.isValidTemperature()) {
          // 第一次用到时才建对象
          if (record == null) {
            // 按模式建一条空记录
            record = new GenericData.Record(SCHEMA);
          }
          // 年份要用整数，因为后面要按它分组
          record.put("year", parser.getYearInt());
          // 气温
          record.put("temperature", parser.getAirTemperature());
          // 站号
          record.put("stationId", parser.getStationId());
          // 键是年份，值是那条 Avro 记录
          emitter.emit(Pair.of(parser.getYearInt(), record));
        }
      }
    };
  }

}
