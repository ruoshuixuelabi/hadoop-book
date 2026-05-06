/*
 * 本文件演示了使用Generic Avro API处理Sqoop导入的Avro格式数据
 *
 * 与MaxWidgetId.java的区别：
 * - MaxWidgetId使用Sqoop生成的特定Widget类
 * - 本文件使用GenericRecord，可以处理任意Avro模式的Avro数据
 *
 * 适用场景：
 * - 不知道具体数据结构的Avro文件
 * - 需要处理多种不同类型Avro数据的通用工具
 * - Avro模式可能变化的灵活数据处理
 */

// cc Avro核心库导入
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

// vv Hadoop核心库导入
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用Generic Avro API从Sqoop导入的Avro格式数据中找出最大ID的widget。
 * <p>
 * 本程序与MaxWidgetId.java功能相同，但使用了不同的实现方式：<br/>
 * - 不依赖Sqoop生成的特定类<br/>
 * - 使用GenericRecord和Avro的通用API处理数据
 * <p>
 * 技术要点：<br/>
 * 1. 使用AvroKeyInputFormat/AvroKeyOutputFormat处理Avro数据<br/>
 * 2. 使用GenericRecord表示任意Avro数据，无需编译时类型<br/>
 * 3. 从Avro文件的header中读取schema信息
 * <p>
 * Avro序列化格式：<br/>
 * - Avro数据通常存储在.avro扩展名的文件中<br/>
 * - 文件包含schema定义和数据块<br/>
 * - 支持压缩，提高存储效率
 *
 * @see MaxWidgetId 使用Sqoop生成类的版本
 */
public class MaxWidgetIdGenericAvro extends Configured implements Tool {

    /**
     * Mapper：使用GenericRecord处理Avro格式的widget数据。
     * <p/>
     * 输入：AvroKey<GenericRecord>（包含Avro数据的key）, NullWritable（value为空）<br/>
     * 输出：AvroKey<Long>（widget的id）, AvroValue<GenericRecord>（完整的widget记录）
     * <p/>
     * 设计说明：<br/>
     * - 使用GenericRecord而非特定类，保持灵活性<br/>
     * - AvroKey/AvroValue是Hadoop与Avro之间的桥梁类型
     */
    public static class MaxWidgetMapper extends Mapper<AvroKey<GenericRecord>, NullWritable,
            AvroKey<Long>, AvroValue<GenericRecord>> {

        // 存储当前map内的最大widget
        private GenericRecord maxWidget;

        /**
         * Map方法：处理每条Avro格式的widget记录。
         * <p/>
         * 从AvroKey中获取GenericRecord（通用的Avro数据表示）<br/>
         * 通过字段名"id"获取值，而不是通过getter方法
         *
         * @param key 包含widget数据的AvroKey
         * @param value 固定为NullWritable（Avro数据在key中）
         * @param context 上下文，用于输出结果
         */
        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            // 从AvroKey中获取GenericRecord（通用的Avro数据表示）
            GenericRecord widget = key.datum();
            // 通过字段名"id"获取值，而不是通过getter方法
            Integer id = (Integer) widget.get("id");
            if (id != null) {
                if (maxWidget == null
                        || id > (Integer) maxWidget.get("id")) {
                    maxWidget = widget;
                }
            }
        }

        /**
         * Cleanup方法：输出当前map的最大widget。
         * <p/>
         * 输出到固定key=0，使得所有mapper结果发送到同一reducer
         *
         * @param context 上下文
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 输出到固定key=0，使得所有mapper结果发送到同一reducer
            context.write(new AvroKey(0L), new AvroValue<GenericRecord>(maxWidget));
        }
    }

    /**
     * Reducer：汇总所有mapper的结果，找出全局最大ID的widget。
     * <p/>
     * 输入：AvroKey<Long>（固定key=0）, AvroValue<GenericRecord>（各mapper的最大widget）<br/>
     * 输出：AvroKey<GenericRecord>（全局最大widget）, NullWritable
     */
    public static class MaxWidgetReducer
            extends Reducer<AvroKey<Long>, AvroValue<GenericRecord>,
            AvroKey<GenericRecord>, NullWritable> {

        /**
         * Reduce方法：遍历所有mapper的输出，找出最大ID。
         * <p/>
         * 遍历所有mapper的输出，找出最大ID<br/>
         * 需要复制record，因为迭代器可能返回同一个对象
         *
         * @param key 固定值0
         * @param values 各mapper最大widget的迭代器
         * @param context 上下文
         */
        @Override
        protected void reduce(AvroKey<Long> key, Iterable<AvroValue<GenericRecord>>
                values, Context context) throws IOException, InterruptedException {
            GenericRecord maxWidget = null;
            for (AvroValue<GenericRecord> value : values) {
                GenericRecord record = value.datum();
                if (maxWidget == null
                        || (Integer) record.get("id") > (Integer) maxWidget.get("id")) {
                    // 需要复制record，因为迭代器可能返回同一个对象
                    maxWidget = copy(record);
                }
            }
            context.write(new AvroKey(maxWidget), NullWritable.get());
        }

        /**
         * 创建GenericRecord的深拷贝。
         * <p/>
         * 由于GenericRecord可能是只读的，需要创建一个可修改的副本<br/>
         * 用于在比较过程中保存最大值
         *
         * @param record 要复制的原始记录
         * @return 具有相同数据的新GenericRecord
         */
        private GenericRecord copy(GenericRecord record) {
            // 获取record的schema定义
            Schema schema = record.getSchema();
            // 创建同类型的新记录
            GenericRecord copy = new GenericData.Record(schema);
            // 复制所有字段值
            for (Schema.Field f : schema.getFields()) {
                copy.put(f.name(), record.get(f.name()));
            }
            return copy;
        }
    }

    /**
     * 配置并执行MapReduce作业。
     * <p/>
     * Avro MapReduce作业的特殊配置：<br/>
     * 1. 使用AvroJob工具类设置Avro特定的schema<br/>
     * 2. 使用AvroKeyInputFormat和AvroKeyOutputFormat<br/>
     * 3. 需要预先读取schema以配置作业
     *
     * @param args 命令行参数（未使用）
     * @return 作业成功返回0，失败返回1
     * @throws Exception 如果配置或执行出错
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "Max widget ID");
        job.setJarByClass(getClass());

        // 设置用户类优先加载，避免类路径冲突
        job.getConfiguration().setBoolean(
                Job.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        // 设置输入输出路径
        Path inputDir = new Path("widgets");
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

        // 从Avro文件中读取schema
        Schema schema = readSchema(inputDir, getConf());

        // 配置Avro特定的输出类型
        // map输出key为Long类型（用于分组）
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.LONG));
        // map输出value为原始widget schema
        AvroJob.setMapOutputValueSchema(job, schema);
        // 最终输出为widget schema
        AvroJob.setOutputKeySchema(job, schema);

        // 设置Avro格式的输入输出类
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);

        // 设置Mapper和Reducer
        job.setMapperClass(MaxWidgetMapper.class);
        job.setReducerClass(MaxWidgetReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * 从Avro文件目录中读取第一个文件的schema。
     * <p/>
     * Avro文件是自描述的，schema信息存储在文件头中<br/>
     * 本方法打开第一个.avro文件并提取其schema
     * <p/>
     * 设计说明：<br/>
     * - 假设所有widget文件使用相同的schema<br/>
     * - 使用FsInput直接读取HDFS上的Avro文件
     *
     * @param inputDir 包含Avro文件的目录
     * @param conf Hadoop配置
     * @return Avro schema对象
     * @throws IOException 如果文件读取失败
     */
    private Schema readSchema(Path inputDir, Configuration conf) throws IOException {
        FsInput fsInput = null;
        FileReader<Object> reader = null;
        try {
            // 打开Avro文件（假设第一个分片为part-m-00000.avro）
            fsInput = new FsInput(new Path(inputDir, "part-m-00000.avro"), conf);
            // 创建GenericDatumReader用于读取GenericRecord
            reader = DataFileReader.openReader(fsInput, new GenericDatumReader<Object>());
            // 从文件头获取schema
            return reader.getSchema();
        } finally {
            // 确保资源被正确关闭
            IOUtils.closeStream(fsInput);
            IOUtils.closeStream(reader);
        }
    }

    /**
     * 程序入口。
     *
     * @param args 命令行参数
     * @throws Exception 如果执行出错
     */
    public static void main(String [] args) throws Exception {
        int ret = ToolRunner.run(new MaxWidgetIdGenericAvro(), args);
        System.exit(ret);
    }
}
