/*
 * Copyright 2008 Last.fm.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fm.last.hadoop.programs.labs.trackstats;

/*
 * TrackStatisticsProgram - 音乐曲目统计分析程序
 *
 * 本程序演示了如何从用户听歌行为日志中提取有价值的统计信息。
 * 它串联运行多个MapReduce作业，每个作业负责特定的数据转换任务。
 *
 * 解决的问题：
 * - 计算每首曲目的独立听众数量（去重）
 * - 统计每首曲目的播放次数、跳过次数等指标
 * - 展示如何在Hadoop中实现复杂的数据处理流水线
 *
 * 输入数据格式（空格分隔）：
 * user_id track_id scrobbles radio skips
 * 示例：12345 98765 3 1 0
 * 表示用户12345播放曲目98765共3次（直接播放），收音机播放1次，跳过0次
 *
 * 输出数据格式：
 * track_id listeners plays scrobbles radioPlays skips
 * 表示曲目98765有N个独立听众，总播放M次，直接播放X次，收音机播放Y次，跳过Z次
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

// Apache Commons Logging - 用于程序运行日志记录
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Hadoop核心库
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// Hadoop MapReduce旧API（org.apache.hadoop.mapred）
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

// Hadoop作业控制
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

// Hadoop内置的Mapper和Reducer实现
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

// 本地项目的数据记录类
import fm.last.hadoop.io.records.TrackStats;

/**
 * 曲目统计分析程序 - 从用户听歌日志中提取播放统计信息。
 *
 * 本程序展示了Hadoop在实际音乐数据分析中的应用：
 * 1. 处理大规模用户听歌行为日志
 * 2. 计算每首曲目的独立听众数量
 * 3. 统计播放次数、跳过次数等指标
 * 4. 使用作业依赖关系构建数据处理流水线
 *
 * 程序架构：
 * 整个分析流程由三个MapReduce作业组成，它们依次执行：
 * 1. 独立听众计数作业 - 统计每首曲目的独立听众数量
 * 2. 数据汇总作业 - 累加每首曲目的播放/跳过次数
 * 3. 数据合并作业 - 将前两个作业的结果合并为最终输出
 *
 * 使用示例：
 * <pre>
 * hadoop jar trackstats.jar /input/listening-data /output/track-stats
 * </pre>
 *
 * 注意：本程序使用Hadoop旧API（mapred包），而非新API（mapreduce包）
 *
 * @see TrackStats
 */
public class TrackStatisticsProgram {

    // 日志记录器 - 用于监控程序运行状态
    public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

    // values below indicate position in raw data for each value
    // 输入数据字段位置索引 - 用于解析原始数据
    // 原始数据格式：user_id track_id scrobbles radio skips
    private static final int COL_USERID = 0;      // 用户ID在第0列
    private static final int COL_TRACKID = 1;     // 曲目ID在第1列
    private static final int COL_SCROBBLES = 2;   // 直接播放次数在第2列
    private static final int COL_RADIO = 3;       // 收音机播放次数在第3列
    private static final int COL_SKIP = 4;        // 跳过次数在第4列

    // Hadoop配置对象
    private Configuration conf;

    /**
     * 构造函数 - 使用默认配置创建程序实例。
     */
    public TrackStatisticsProgram() {
        this.conf = new Configuration();
    }

    /**
     * Hadoop计数器枚举 - 用于跟踪处理过程中的异常情况。
     *
     * 在大数据处理中，经常会遇到各种格式错误或异常数据，
     * 使用计数器可以在作业完成后查看这些异常的数量，
     * 而不是在处理过程中输出日志（那样会影响性能）
     */
    private enum COUNTER_KEYS {
        INVALID_LINES,    // 无效行（空行或格式错误）
        NOT_LISTEN        // 未收听（播放次数和收音机播放都为0）
    };

    // ==================== Mapper类 ====================

    /**
     * 独立听众Mapper - 从听歌日志中提取用户-曲目对。
     *
     * 输入：原始文本行（空格分隔的听歌记录）
     * 输出：(track_id, user_id) 对
     *
     * 工作原理：
     * - 每行代表一个用户对某曲目的一次播放记录
     * - 一个用户可能对同一曲目有多条记录
     * - Mapper输出所有user_id，后续Reducer会对同一曲目的user_id去重
     *
     * 设计目的：
     * - 找出每首曲目有哪些用户听过
     * - Reducer使用Set数据结构自动去除重复的user_id
     */
    public static class UniqueListenersMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, IntWritable, IntWritable> {

        /**
         * Map方法 - 解析每行听歌记录并输出用户-曲目对。
         *
         * @param position 行在文件中的偏移量（未使用）
         * @param rawLine 原始听歌记录行
         * @param output 收集器，用于输出(user_id, track_id)对
         * @param reporter 作业Reporter，用于更新计数器和状态
         */
        public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, IntWritable> output,
                Reporter reporter) throws IOException {

            String line = (rawLine).toString();
            // 检查空行 - 忽略空行并增加无效行计数器
            if (line.trim().isEmpty()) { // if the line is empty, report error and ignore
                reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
                return;
            }

            // 按空格分割原始数据
            String[] parts = line.split(" "); // raw data is whitespace delimited
            try {
                // 解析播放数据
                int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
                int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);

                // 验证：用户是否真的听过这首歌
                if (scrobbles <= 0 && radioListens <= 0) {
                    // if track somehow is marked with zero plays, report error and ignore
                    // 如果播放次数为0，可能是数据问题，跳过
                    reporter.incrCounter(COUNTER_KEYS.NOT_LISTEN, 1);
                    return;
                }

                // if we get to here then user has listened to track, so output user id against track id
                // 用户听过该曲目，输出(track_id, user_id)对
                IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
                IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
                output.collect(trackId, userId);
            } catch (NumberFormatException e) {
                // 数据格式错误，增加计数器并记录
                reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
                reporter.setStatus("Invalid line in listening data: " + rawLine);
                return;
            }
        }
    }

    /**
     * 独立听众Combiner - 在Map端预先去除重复的用户ID。
     *
     * Combiner是Mapper和Reducer之间的优化组件：
     * - 运行在Map节点上，对同一Mapper的输出进行局部聚合
     * - 减少网络传输数据量
     * - 必须产生与Reducer相同格式的输出
     *
     * 本Combiner使用HashSet对同一曲目的user_id进行去重
     */
    public static class UniqueListenersCombiner extends MapReduceBase implements
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        /**
         * Combiner方法 - 对Mapper输出进行局部去重。
         *
         * @param trackId 曲目ID（作为key）
         * @param values 该曲目的所有用户ID（可能有重复）
         * @param output 收集器，输出去重后的(user_id, track_id)对
         * @param reporter Reporter
         */
        public void reduce(IntWritable trackId, Iterator<IntWritable> values,
                OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

            Set<IntWritable> userIds = new HashSet<IntWritable>();
            while (values.hasNext()) {
                IntWritable userId = values.next();
                if (!userIds.contains(userId)) {
                    // if this user hasn't already been marked as listening to the track, add them to set and output them
                    // 用户ID不在集合中，说明是新的独立听众
                    userIds.add(new IntWritable(userId.get()));
                    output.collect(trackId, userId);
                }
            }
        }
    }

    /**
     * 独立听众Reducer - 统计每首曲目的独立听众数量。
     *
     * 由于Combiner已经进行了局部去重，Reducer收到的user_id都是唯一的
     * 所以Reducer只需要统计收到的user_id数量即可
     */
    public static class UniqueListenersReducer extends MapReduceBase implements
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        /**
         * Reducer方法 - 统计独立听众数量。
         *
         * @param trackId 曲目ID
         * @param values 该曲目的所有独立用户ID
         * @param output 输出(track_id, 独立听众数量)
         * @param reporter Reporter
         */
        public void reduce(IntWritable trackId, Iterator<IntWritable> values,
                OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {

            Set<Integer> userIds = new HashSet<Integer>();
            // add all userIds to the set, duplicates automatically removed (set contract)
            // 将所有用户ID加入Set，自动去重
            while (values.hasNext()) {
                IntWritable userId = values.next();
                userIds.add(Integer.valueOf(userId.get()));
            }
            // output trackId -> number of unique listeners per track
            // 输出曲目ID和对应的独立听众数量
            output.collect(trackId, new IntWritable(userIds.size()));
        }

    }

    // ==================== 数据汇总作业 ====================

    /**
     * 数据汇总Mapper - 统计每首曲目的播放/跳过次数。
     *
     * 输入：原始文本行
     * 输出：(track_id, TrackStats)
     *
     * 注意：这里将listeners设为0，因为这个值会在后续作业中合并
     */
    public static class SumMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TrackStats> {

        /**
         * Map方法 - 解析听歌记录并输出TrackStats对象。
         *
         * @param position 行偏移量
         * @param rawLine 原始记录
         * @param output 输出(track_id, TrackStats)
         * @param reporter Reporter
         */
        public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, TrackStats> output,
                Reporter reporter) throws IOException {

            String line = (rawLine).toString();
            if (line.trim().isEmpty()) { // ignore empty lines
                reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
                return;
            }

            String[] parts = line.split(" ");
            try {
                int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
                int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
                int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
                int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);

                // set number of listeners to 0 (this is calculated later) and other values as provided in text file
                // listeners设为0，实际值会在merge作业中从独立听众作业的结果获取
                // plays = scrobbles + radio（总播放次数 = 直接播放 + 收音机播放）
                TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
                output.collect(new IntWritable(trackId), trackstat);
            } catch (NumberFormatException e) {
                reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
                log.warn("Invalid line in listening data: " + rawLine);
            }
        }
    }

    /**
     * 数据汇总Reducer - 累加同一曲目的所有播放/跳过次数。
     */
    public static class SumReducer extends MapReduceBase implements
            Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

        /**
         * Reducer方法 - 累加TrackStats的各项指标。
         *
         * @param trackId 曲目ID
         * @param values 该曲目的所有TrackStats对象
         * @param output 输出汇总后的TrackStats
         * @param reporter Reporter
         */
        @Override
        public void reduce(IntWritable trackId, Iterator<TrackStats> values,
                OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {

            // holds the totals for this track
            // 初始化一个空的TrackStats用于累加
            TrackStats sum = new TrackStats();
            while (values.hasNext()) {
                TrackStats trackStats = (TrackStats) values.next();
                // 累加各项指标
                sum.setListeners(sum.getListeners() + trackStats.getListeners());
                sum.setPlays(sum.getPlays() + trackStats.getPlays());
                sum.setSkips(sum.getSkips() + trackStats.getSkips());
                sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
                sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
            }
            output.collect(trackId, sum);
        }
    }

    // ==================== 数据合并作业 ====================

    /**
     * 合并听众数量Mapper - 将独立听众数量转换为TrackStats。
     *
     * 这个Mapper的作用是将独立听众作业的输出转换为TrackStats格式，
     * 以便与数据汇总作业的输出进行合并
     */
    public static class MergeListenersMapper extends MapReduceBase implements
            Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

        /**
         * Map方法 - 将独立听众数量包装成TrackStats。
         *
         * @param trackId 曲目ID
         * @param uniqueListenerCount 独立听众数量
         * @param output 输出(track_id, TrackStats)
         * @param reporter Reporter
         */
        public void map(IntWritable trackId, IntWritable uniqueListenerCount,
                OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {

            TrackStats trackStats = new TrackStats();
            trackStats.setListeners(uniqueListenerCount.get());
            output.collect(trackId, trackStats);
        }
    }

    // ==================== 作业配置方法 ====================

    /**
     * 创建独立听众计数作业的配置。
     *
     * 作业流程：
     * 1. 读取原始听歌日志
     * 2. Mapper输出(user_id, track_id)对
     * 3. Combiner进行局部去重
     * 4. Reducer统计每个曲目的独立听众数量
     * 5. 输出到SequenceFile格式（供后续作业使用）
     *
     * @param inputDir 原始听歌数据目录
     * @return 配置好的JobConf
     */
    private JobConf getUniqueListenersJobConf(Path inputDir) {
        log.info("Creating configuration for unique listeners Job");

        // output results to a temporary intermediate folder, this will get deleted by start() method
        // 中间结果输出目录，程序运行完后会删除
        Path uniqueListenersOutput = new Path("uniqueListeners");

        JobConf conf = new JobConf(TrackStatisticsProgram.class);
        conf.setOutputKeyClass(IntWritable.class); // track id - 输出key为曲目ID
        conf.setOutputValueClass(IntWritable.class); // number of unique listeners - 输出value为听众数量
        conf.setInputFormat(TextInputFormat.class); // raw listening data - 输入为原始文本数据
        conf.setOutputFormat(SequenceFileOutputFormat.class); // 使用SequenceFile格式存储中间结果
        conf.setMapperClass(UniqueListenersMapper.class);
        conf.setCombinerClass(UniqueListenersCombiner.class); // 使用Combiner减少网络传输
        conf.setReducerClass(UniqueListenersReducer.class);

        FileInputFormat.addInputPath(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, uniqueListenersOutput);
        conf.setJobName("uniqueListeners");
        return conf;
    }

    /**
     * 创建数据汇总作业的配置。
     *
     * 作业流程：
     * 1. 读取原始听歌日志
     * 2. Mapper输出(track_id, TrackStats)，其中listeners=0
     * 3. Reducer累加所有播放/跳过次数
     * 4. 输出到SequenceFile格式
     *
     * @param inputDir 原始听歌数据目录
     * @return 配置好的JobConf
     */
    private JobConf getSumJobConf(Path inputDir) {
        log.info("Creating configuration for sum job");
        // output results to a temporary intermediate folder, this will get deleted by start() method
        Path playsOutput = new Path("sum");

        JobConf conf = new JobConf(TrackStatisticsProgram.class);
        conf.setOutputKeyClass(IntWritable.class); // track id
        conf.setOutputValueClass(TrackStats.class); // statistics for a track
        conf.setInputFormat(TextInputFormat.class); // raw listening data
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setMapperClass(SumMapper.class);
        conf.setCombinerClass(SumReducer.class); // Combiner和Reducer逻辑相同，可以复用
        conf.setReducerClass(SumReducer.class);

        FileInputFormat.addInputPath(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, playsOutput);
        conf.setJobName("sum");
        return conf;
    }

    /**
     * 创建数据合并作业的配置。
     *
     * 这是最复杂的作业，使用MultipleInputs处理两个不同的输入源：
     * 1. sum作业的输出（曲目播放统计）
     * 2. uniqueListeners作业的输出（独立听众数量）
     *
     * 两个输入源都使用相同的数据结构(IntWritable, TrackStats)，
     * Reducer会将它们合并：listeners字段取独立听众值，其他字段累加
     *
     * @param outputPath 最终输出目录
     * @param sumInputDir sum作业的输出目录
     * @param listenersInputDir uniqueListeners作业的输出目录
     * @return 配置好的JobConf
     */
    private JobConf getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) {
        log.info("Creating configuration for merge job");
        JobConf conf = new JobConf(TrackStatisticsProgram.class);
        conf.setOutputKeyClass(IntWritable.class); // track id
        conf.setOutputValueClass(TrackStats.class); // overall track statistics
        conf.setCombinerClass(SumReducer.class); // safe to re-use reducer as a combiner here
        conf.setReducerClass(SumReducer.class);
        conf.setOutputFormat(TextOutputFormat.class); // 最终输出为文本格式

        FileOutputFormat.setOutputPath(conf, outputPath);

        // 配置多个输入源，每个可以使用不同的InputFormat和Mapper
        MultipleInputs.addInputPath(conf, sumInputDir, SequenceFileInputFormat.class, IdentityMapper.class);
        MultipleInputs.addInputPath(conf, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
        conf.setJobName("merge");
        return conf;
    }

    // ==================== 主程序入口 ====================

    /**
     * 启动曲目统计分析程序。
     *
     * 程序执行流程：
     * 1. 创建三个作业的配置
     * 2. 建立作业依赖关系（merge依赖于前两个作业）
     * 3. 使用JobControl管理作业执行
     * 4. 等待所有作业完成
     * 5. 清理中间结果目录
     *
     * @param inputDir 原始听歌数据目录
     * @param outputDir 最终输出目录
     * @throws IOException 如果文件系统操作失败
     */
    public void start(Path inputDir, Path outputDir) throws IOException {
        FileSystem fs = FileSystem.get(this.conf);

        // ---- 作业1：独立听众计数 ----
        JobConf uniqueListenersConf = getUniqueListenersJobConf(inputDir);
        Path listenersOutputDir = FileOutputFormat.getOutputPath(uniqueListenersConf);
        Job listenersJob = new Job(uniqueListenersConf);
        // delete any output that might exist from a previous run of this job
        // 删除可能存在的旧输出目录
        if (fs.exists(FileOutputFormat.getOutputPath(uniqueListenersConf))) {
            fs.delete(FileOutputFormat.getOutputPath(uniqueListenersConf), true);
        }

        // ---- 作业2：数据汇总 ----
        JobConf sumConf = getSumJobConf(inputDir);
        Path sumOutputDir = FileOutputFormat.getOutputPath(sumConf);
        Job sumJob = new Job(sumConf);
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(sumConf))) {
            fs.delete(FileOutputFormat.getOutputPath(sumConf), true);
        }

        // ---- 作业3：数据合并（依赖前两个作业） ----
        // the merge job depends on the other two jobs
        // 建立作业依赖关系，确保按顺序执行
        ArrayList<Job> mergeDependencies = new ArrayList<Job>();
        mergeDependencies.add(listenersJob);
        mergeDependencies.add(sumJob);
        JobConf mergeConf = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
        Job mergeJob = new Job(mergeConf, mergeDependencies);
        // delete any output that might exist from a previous run of this job
        if (fs.exists(FileOutputFormat.getOutputPath(mergeConf))) {
            fs.delete(FileOutputFormat.getOutputPath(mergeConf), true);
        }

        // store the output paths of the intermediate jobs so this can be cleaned up after a successful run
        // 记录中间结果目录路径，作业成功后清理
        List<Path> deletePaths = new ArrayList<Path>();
        deletePaths.add(FileOutputFormat.getOutputPath(uniqueListenersConf));
        deletePaths.add(FileOutputFormat.getOutputPath(sumConf));

        // ---- 使用JobControl管理作业执行 ----
        JobControl control = new JobControl("TrackStatisticsProgram");
        control.addJob(listenersJob);
        control.addJob(sumJob);
        control.addJob(mergeJob);

        // execute the jobs
        // 在独立线程中启动作业控制，避免阻塞主线程
        try {
            Thread jobControlThread = new Thread(control, "jobcontrol");
            jobControlThread.start();
            // 轮询检查作业完成状态，每秒检查一次
            while (!control.allFinished()) {
                Thread.sleep(1000);
            }
            // 检查是否有作业失败
            if (control.getFailedJobs().size() > 0) {
                throw new IOException("One or more jobs failed");
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while waiting for job control to finish", e);
        }

        // remove intermediate output paths
        // 清理中间结果目录，保留最终输出
        for (Path deletePath : deletePaths) {
            fs.delete(deletePath, true);
        }
    }

    /**
     * 设置程序的Configuration。
     *
     * @param conf 新的Configuration对象
     */
    public void setConf(Configuration conf) {
        this.conf = conf; // this will usually only be set by unit test.
    }

    /**
     * 获取程序的Configuration。
     *
     * @return 当前的Configuration对象
     */
    public Configuration getConf() {
        return conf;
    }

    /**
     * 命令行入口。
     *
     * 用法：TrackStatisticsProgram <input_directory> <output_directory>
     *
     * 参数说明：
     * - input_directory: 包含原始听歌数据的目录
     * - output_directory: 最终统计结果的输出目录
     *
     * @param args 命令行参数
     * @throws IOException 如果程序执行出错
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            log.info("Args: <input directory> <output directory>");
            return;
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        log.info("Running on input directories: " + inputPath);
        TrackStatisticsProgram listeners = new TrackStatisticsProgram();
        listeners.start(inputPath, outputDir);
    }

}
