package oldapi;

import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Avro 投影 (Projection) 示例程序。
 *
 * <p>演示如何在 MapReduce 中使用 Avro 投影功能，
 * 只读取 schema 中的特定字段，减少网络传输和计算开销。
 *
 * <p>用法:
 * <pre>
 * hadoop jar avro-examples.jar oldapi.AvroProjection [input] [output] [schema-file]
 * </pre>
 *
 * <p>注意: 此程序禁用 Reduce 任务 (numReduceTasks=0)，
 * 数据直接从 Mapper 输出到最终结果。
 */
public class AvroProjection extends Configured implements Tool {

    /**
     * 配置并运行 Avro 投影作业。
     *
     * <p>此作业演示 Avro 的 Schema 投影功能:
     * <ul>
     *   <li>设置输入 schema</li>
     *   <li>Map 输出使用投影后的 schema</li>
     *   <li>最终输出也使用投影后的 schema</li>
     * </ul>
     *
     * <p>当使用投影时，只有 schema 中声明的字段会被读取和处理，
     * 其他字段会被忽略，从而提升性能。
     *
     * @param args 命令行参数: [input] [output] [schema-file]
     * @return 作业退出码
     */
    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
            getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String input = args[0];
        String output = args[1];
        String schemaFile = args[2];

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("Avro projection");

        FileInputFormat.addInputPath(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        // 解析 schema 文件
        Schema schema = new Schema.Parser().parse(new File(schemaFile));
        // 设置 Avro 的输入、Map输出、最终输出 schema
        AvroJob.setInputSchema(conf, schema);
        AvroJob.setMapOutputSchema(conf, schema);
        AvroJob.setOutputSchema(conf, schema);
        // 禁用 Reduce 任务
        conf.setNumReduceTasks(0);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AvroProjection(), args);
        System.exit(exitCode);
    }

}