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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import fm.last.hadoop.io.records.TrackStats;

/**
 * 从原始收听数据里算出各种跟歌曲有关的统计数字
 *
 * <p>这是第 22 章的案例研究，代码来自 Last.fm 的开源项目，
 * 展示的是真实业务里的 MapReduce 作业是怎么组织的
 * <p>整个程序由三个作业串起来：
 * 先算每首歌有多少人听过（把重复的听众去掉），
 * 再把播放、收藏、跳过这些计数累加起来，
 * 最后把两份结果合并成一条完整的统计
 *
 * Program that calculates various track-related statistics from raw listening data.
 */
public class TrackStatisticsProgram {

  // 日志对象，出错时的细节都往这里写
  public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

  // values below indicate position in raw data for each value
  // 补充：下面几个常量表示每个值在原始数据里的列号

  // 用户 id 所在列
  private static final int COL_USERID = 0;
  // 歌曲 id 所在列
  private static final int COL_TRACKID = 1;
  // 收藏次数所在列
  private static final int COL_SCROBBLES = 2;
  // 电台播放次数所在列
  private static final int COL_RADIO = 3;
  // 跳过次数所在列
  private static final int COL_SKIP = 4;

  // 本程序用的 Hadoop 配置
  private Configuration conf;

  /**
   * 用默认配置新建一个程序
   *
   * <p>不另外指定配置时，Hadoop 会去 classpath 里找 {@code core-site.xml}
   * 这些默认配置文件
   *
   * Constructs a new TrackStatisticsProgram, using a default Configuration.
   */
  public TrackStatisticsProgram() {
    this.conf = new Configuration();
  }

  /**
   * Hadoop 错误计数器
   *
   * <p>统计两类异常数据：格式不对的行，和「其实根本没在听歌」的行
   *
   * Enumeration for Hadoop error counters.
   */
  private enum COUNTER_KEYS {
    INVALID_LINES, NOT_LISTEN
  };

  /**
   * Mapper：把原始收听数据映射成「歌曲 id → 听众 id」
   *
   * <p>输出的每一对后面都会被去重，
   * 所以这里同一个听众听了几次就输出几次也没关系
   *
   * Mapper that takes in raw listening data and outputs the number of unique listeners per track.
   */
  public static class UniqueListenersMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {

      // 原始数据是一行一条收听记录
      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // if the line is empty, report error and ignore
        // 补充：空行不算一次有效收听，记一笔错误计数就跳过
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" "); // raw data is whitespace delimited
      // 补充：原始数据用空白符分隔，切开就是各个字段
      try {
        // 取收藏次数
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        // 两个数都是 0，说明这一条其实不算一次收听
        if (scrobbles <= 0 && radioListens <= 0) {
          // if track somehow is marked with zero plays, report error and ignore
          // 补充：记一次错误计数，然后跳过这条
          reporter.incrCounter(COUNTER_KEYS.NOT_LISTEN, 1);
          return;
        }
        // if we get to here then user has listened to track, so output user id against track id
        // 补充：这一条确实是收听，输出「歌曲 id → 听众 id」
        // 输出用歌曲 id 当键
        IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
        // 用听众 id 当值
        IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
        // 输出「歌曲 id → 听众 id」
        output.collect(trackId, userId);
      // 数字解析不出来，说明这一行格式不对
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        // 把出错的那一行写进作业状态，方便到 UI 上查
        reporter.setStatus("Invalid line in listening data: " + rawLine);
        return;
      }
    }
  }

  /**
   * Combiner：在 Map 端先做一次去重
   *
   * <p>同一个听众听同一首歌很多次时，
   * 在 Map 端就把重复的去掉，能少往网络上搬很多数据
   *
   * Combiner that improves efficiency by removing duplicate user ids from mapper output.
   */
  public static class UniqueListenersCombiner extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      // 用集合记住已经见过的听众 id
      Set<IntWritable> userIds = new HashSet<IntWritable>();
      while (values.hasNext()) {
        // 取下一个听众 id
        IntWritable userId = values.next();
        // 没见过的才算一个新听众
        if (!userIds.contains(userId)) {
          // if this user hasn't already been marked as listening to the track, add them to set and output them
          // 补充：记进集合，并且输出出去
          // 存进去，后面遇到同一个 id 就不会再输出了
          userIds.add(new IntWritable(userId.get()));
          // 输出「歌曲 id → 听众 id」
          output.collect(trackId, userId);
        }
      }
    }
  }

  /**
   * Reducer：把同一首歌的听众 id 彻底去重
   *
   * <p>输出的是「歌曲 id → 去重之后的听众人数」
   *
   * Reducer that outputs only unique listener ids per track (i.e. it removes any duplicated). Final output is number of
   * unique listeners per track.
   */
  public static class UniqueListenersReducer extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      // 用集合去重
      Set<Integer> userIds = new HashSet<Integer>();
      // add all userIds to the set, duplicates automatically removed (set contract)
      // 补充：全部塞进集合，重复的会被集合自动去掉
      // 把这个键对应的所有值收完
      while (values.hasNext()) {
        IntWritable userId = values.next();
        // 塞进集合
        userIds.add(Integer.valueOf(userId.get()));
      }
      // output trackId -> number of unique listeners per track
      // 补充：输出「歌曲 id → 听众人数」
      // 集合大小就是去重之后的人数
      output.collect(trackId, new IntWritable(userIds.size()));
    }

  }

  /**
   * Mapper：把原始收听数据汇总成一条「听众人数还没填」的统计
   *
   * <p>听众人数要等去重之后才知道，所以这里先填 0，
   * 留给后面的 merge 作业补上
   *
   * Mapper that summarizes various statistics per track. Input is raw listening data, output is a partially filled in
   * TrackStatistics object per track id.
   */
  public static class SumMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TrackStats> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, TrackStats> output,
        Reporter reporter) throws IOException {

      // 原始数据是一行一条收听记录
      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // ignore empty lines
        // 补充：空行跳过，记一笔错误计数
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      // 按空白符切成各个字段
      String[] parts = line.split(" ");
      try {
        // 歌曲 id
        int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
        // 收藏次数
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        // 电台播放次数
        int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        // 跳过次数
        int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);
        // set number of listeners to 0 (this is calculated later) and other values as provided in text file
        // 补充：听众人数先填 0，等去重作业算出来后再补
        // 播放次数 = 收藏次数 + 电台播放次数
        TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
        // 输出「歌曲 id → 统计对象」
        output.collect(new IntWritable(trackId), trackstat);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        // 记一条警告日志
        log.warn("Invalid line in listening data: " + rawLine);
      }
    }
  }

  /**
   * Reducer：把同一首歌的各项计数累加起来
   *
   * <p>它同时也被拿来当 Combiner 用，
   * 因为累加这种操作在 Map 端做和在 Reduce 端做结果是一样的
   *
   * Sum up the track statistics per track. Output is a TrackStatistics object per track id.
   */
  public static class SumReducer extends MapReduceBase implements
      Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

    @Override
    public void reduce(IntWritable trackId, Iterator<TrackStats> values,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats sum = new TrackStats(); // holds the totals for this track
      // 补充：累加结果先放在这里
      while (values.hasNext()) {
        // 取下一个统计对象
        TrackStats trackStats = (TrackStats) values.next();
        // 听众人数累加
        sum.setListeners(sum.getListeners() + trackStats.getListeners());
        // 播放次数累加
        sum.setPlays(sum.getPlays() + trackStats.getPlays());
        // 跳过次数累加
        sum.setSkips(sum.getSkips() + trackStats.getSkips());
        // 收藏次数累加
        sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
        // 电台播放次数累加
        sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
      }
      // 输出这一首歌的最终统计
      output.collect(trackId, sum);
    }
  }

  /**
   * Mapper：把「听众人数」包装成一个 {@code TrackStats}
   *
   * <p>这样 merge 作业就能把两个来源的数据用同一种对象累加
   *
   * Mapper that takes the number of listeners for a track and converts this to a TrackStats object which is output
   * against each track id.
   */
  public static class MergeListenersMapper extends MapReduceBase implements
      Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

    public void map(IntWritable trackId, IntWritable uniqueListenerCount,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      // 先建一个空的统计对象
      TrackStats trackStats = new TrackStats();
      // 把听众人数填进去，其他字段保持 0
      trackStats.setListeners(uniqueListenerCount.get());
      // 输出「歌曲 id → 统计对象」
      output.collect(trackId, trackStats);
    }
  }

  /**
   * 生成「统计每首歌有多少听众」这个作业的配置
   *
   * <p>输出的键是歌曲 id，值是去重之后的听众人数
   *
   * Create a JobConf for a Job that will calculate the number of unique listeners per track.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @return The unique listeners JobConf.
   */
  private JobConf getUniqueListenersJobConf(Path inputDir) {
    log.info("Creating configuration for unique listeners Job");

    // output results to a temporary intermediate folder, this will get deleted by start() method
    // 中间结果目录，跑完就删
    Path uniqueListenersOutput = new Path("uniqueListeners");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    // 补充：输出的键是歌曲 id
    conf.setOutputValueClass(IntWritable.class); // number of unique listeners
    // 补充：输出的值是去重后的听众人数
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    // 补充：输入是纯文本
    // 输出用 SequenceFile，后面才能当中间结果再读回来
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(UniqueListenersMapper.class);
    conf.setCombinerClass(UniqueListenersCombiner.class);
    conf.setReducerClass(UniqueListenersReducer.class);

    // 指定输入目录
    FileInputFormat.addInputPath(conf, inputDir);
    // 指定输出目录
    FileOutputFormat.setOutputPath(conf, uniqueListenersOutput);
    // 作业名字，失败时在 UI 上就靠它认
    conf.setJobName("uniqueListeners");
    return conf;
  }

  /**
   * 生成「累加各项统计」这个作业的配置
   *
   * Creates a JobConf for a Job that will sum up the TrackStatistics per track.
   * 
   * @param inputDir The path to the folder containing the raw input data files.
   * @return The sum JobConf.
   */
  private JobConf getSumJobConf(Path inputDir) {
    log.info("Creating configuration for sum job");
    // output results to a temporary intermediate folder, this will get deleted by start() method
    // 中间结果目录，跑完就删
    Path playsOutput = new Path("sum");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    // 补充：输出的键是歌曲 id
    conf.setOutputValueClass(TrackStats.class); // statistics for a track
    // 补充：输出的值是一首歌的统计对象
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    // 补充：输入同样是纯文本
    // 输出用 SequenceFile
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(SumMapper.class);
    // 累加型的聚合，在 Map 端也能做
    conf.setCombinerClass(SumReducer.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, playsOutput);
    // 作业名字
    conf.setJobName("sum");
    return conf;
  }

  /**
   * 生成「合并」这个作业的配置
   *
   * <p>它有两个输入：累加作业的结果，和去重作业的结果
   *
   * Creates a JobConf for a Job that will merge the unique listeners and track statistics.
   * 
   * @param outputPath The path for the results to be output to.
   * @param sumInputDir The path containing the data from the sum Job.
   * @param listenersInputDir The path containing the data from the unique listeners job.
   * @return The merge JobConf.
   */
  private JobConf getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) {
    log.info("Creating configuration for merge job");
    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    // 补充：输出的键是歌曲 id
    conf.setOutputValueClass(TrackStats.class); // overall track statistics
    // 补充：输出的值是合并之后的完整统计
    conf.setCombinerClass(SumReducer.class); // safe to re-use reducer as a combiner here
    // 补充：累加同样可以放到 Map 端做
    // 同样用累加的 Reducer
    conf.setReducerClass(SumReducer.class);
    // 最终结果用纯文本输出，方便人直接看
    conf.setOutputFormat(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(conf, outputPath);

    // 第一个输入：累加作业的结果，原样透传
    MultipleInputs.addInputPath(conf, sumInputDir, SequenceFileInputFormat.class, IdentityMapper.class);
    // 第二个输入：去重作业的结果，要包成 TrackStats
    MultipleInputs.addInputPath(conf, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
    // 作业名字
    conf.setJobName("merge");
    return conf;
  }

  /**
   * 启动整个程序
   *
   * <p>三个作业按依赖关系串起来：
   * 先并行跑去重和累加两个作业，等它们都成功了再跑合并作业
   *
   * Start the program.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @param outputPath The path for the results to be output to.
   * @throws IOException If an error occurs retrieving data from the file system or an error occurs running the job.
   */
  public void start(Path inputDir, Path outputDir) throws IOException {
    // 拿到文件系统，用来清理中间结果目录
    FileSystem fs = FileSystem.get(this.conf);

    JobConf uniqueListenersConf = getUniqueListenersJobConf(inputDir);
    Path listenersOutputDir = FileOutputFormat.getOutputPath(uniqueListenersConf);
    // 把去重作业包成 Job 对象
    Job listenersJob = new Job(uniqueListenersConf);
    // delete any output that might exist from a previous run of this job
    // 上一次跑剩的中间结果要先删掉
    if (fs.exists(FileOutputFormat.getOutputPath(uniqueListenersConf))) {
      fs.delete(FileOutputFormat.getOutputPath(uniqueListenersConf), true);
    }

    JobConf sumConf = getSumJobConf(inputDir);
    // 记下累加作业的输出目录，合并作业要用
    Path sumOutputDir = FileOutputFormat.getOutputPath(sumConf);
    // 把累加作业包成 Job 对象
    Job sumJob = new Job(sumConf);
    // delete any output that might exist from a previous run of this job
    // 同样先清掉上一次的残留
    if (fs.exists(FileOutputFormat.getOutputPath(sumConf))) {
      fs.delete(FileOutputFormat.getOutputPath(sumConf), true);
    }

    // the merge job depends on the other two jobs
    // 合并作业依赖前面两个作业
    ArrayList<Job> mergeDependencies = new ArrayList<Job>();
    mergeDependencies.add(listenersJob);
    mergeDependencies.add(sumJob);
    JobConf mergeConf = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
    // 带依赖地创建合并作业
    Job mergeJob = new Job(mergeConf, mergeDependencies);
    // delete any output that might exist from a previous run of this job
    // 最终输出目录也要先清空
    if (fs.exists(FileOutputFormat.getOutputPath(mergeConf))) {
      fs.delete(FileOutputFormat.getOutputPath(mergeConf), true);
    }

    // store the output paths of the intermediate jobs so this can be cleaned up after a successful run
    // 记下跑完要删掉的中间目录
    List<Path> deletePaths = new ArrayList<Path>();
    deletePaths.add(FileOutputFormat.getOutputPath(uniqueListenersConf));
    deletePaths.add(FileOutputFormat.getOutputPath(sumConf));

    // JobControl 用来统一调度这几个作业
    JobControl control = new JobControl("TrackStatisticsProgram");
    control.addJob(listenersJob);
    control.addJob(sumJob);
    control.addJob(mergeJob);

    // execute the jobs
    try {
      // JobControl 得跑在自己的线程里
      Thread jobControlThread = new Thread(control, "jobcontrol");
      jobControlThread.start();
      // 主线程在这里轮询，等所有作业跑完
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      // 有作业失败就当整体失败
      if (control.getFailedJobs().size() > 0) {
        throw new IOException("One or more jobs failed");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for job control to finish", e);
    }

    // remove intermediate output paths
    // 成功的活把中间结果删掉
    for (Path deletePath : deletePaths) {
      fs.delete(deletePath, true);
    }
  }

  /**
   * 设置这个程序用的配置
   *
   * Set the Configuration used by this Program.
   * 
   * @param conf The new Configuration to use by this program.
   */
  public void setConf(Configuration conf) {
    // 补充：一般只有单元测试才会改这个配置
    this.conf = conf; // this will usually only be set by unit test.
  }

  /**
   * 取这个程序用的配置
   *
   * Gets the Configuration used by this program.
   * 
   * @return This program's Configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * 命令行入口
   *
   * <p>两个参数：原始数据的目录、结果输出的目录
   *
   * Main method used to run the TrackStatisticsProgram from the command line. This takes two parameters - first the
   * path to the folder containing the raw input data; and second the path for the data to be output to.
   * 
   * @param args Command line arguments.
   * @throws IOException If an error occurs running the program.
   */
  public static void main(String[] args) throws Exception {
    // 两个参数都得给
    if (args.length < 2) {
      log.info("Args: <input directory> <output directory>");
      return;
    }

    // 原始数据目录
    Path inputPath = new Path(args[0]);
    // 结果输出目录
    Path outputDir = new Path(args[1]);
    log.info("Running on input directories: " + inputPath);
    // 建一个程序对象并跑起来
    TrackStatisticsProgram listeners = new TrackStatisticsProgram();
    listeners.start(inputPath, outputDir);
  }

}
