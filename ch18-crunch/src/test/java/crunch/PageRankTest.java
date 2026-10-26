package crunch;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineExecution;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.test.TemporaryPath;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.crunch.types.avro.Avros.floats;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.Assert.assertEquals;

/**
 * 用 Crunch 实现 PageRank：反复迭代，直到各节点的分数收敛
 *
 * <p>PageRank 的思路是：每个节点把自己的分数平均分给它指向的所有节点，
 * 收到的分数按阻尼因子加权后再加回来，如此迭代直到分数不再变化。
 * 本测试用 Crunch 的 {@code cogroup} 把「节点自己的记录」和「它收到的分数」
 * 放到同一个键下配对，再用 {@code Aggregate.max} 取全体节点中最大的分数变化量
 * 作为收敛判据。
 *
 * <p>这里还有一个关键点：Crunch 的流水线是惰性的，每一轮迭代都必须显式物化，
 * 否则 DAG 会越积越长，既跑不动也看不出中间的收敛过程。
 *
 * <p>测试类实现 {@code Serializable}，是因为实例方法里创建的匿名内部类
 * （它会隐式引用外层实例）需要被序列化后送到集群上执行。
 */
public class PageRankTest implements Serializable {

  /**
   * 临时目录规则：测试方法执行前自动建好临时目录，执行后自动删除
   *
   * <p>标成 {@code transient}，是因为本类实现了 {@code Serializable}，
   * 而 {@code TemporaryPath} 只在本机有效，序列化它没有意义。
   */
  @Rule
  public transient TemporaryPath tmpDir = new TemporaryPath();

  /**
   * 每个节点的 PageRank 状态
   *
   * <p>{@code score} 是当前分数，{@code lastScore} 是上一轮的分数，
   * 两者之差就是这一轮的收敛幅度；{@code urls} 是它指向的所有节点。
   *
   * <p>它用 Avro 的反射模式序列化，所以字段必须是 public，
   * 而且要提供一个公开的无参构造方法，供 Avro 反序列化时创建实例。
   */
  public static class PageRankData {

    /**
     * 当前这一轮的分数
     */
    public float score;

    /**
     * 上一轮的分数，用来判断是否已经收敛
     */
    public float lastScore;

    /**
     * 本节点指向的所有节点
     */
    public List<String> urls;

    /**
     * Avro 反序列化时使用的无参构造方法，必须是 public 的
     */
    public PageRankData() {
    }

    /**
     * 构造一份完整的节点状态
     *
     * @param score 当前分数
     * @param lastScore 上一轮的分数
     * @param urls 本节点指向的所有节点
     */
    public PageRankData(float score, float lastScore, Iterable<String> urls) {
      this.score = score;
      this.lastScore = lastScore;
      this.urls = Lists.newArrayList(urls);
    }

    /**
     * 生成下一轮的状态：新分数写进 score，旧分数挪到 lastScore
     *
     * <p>把上一轮的分数留来，是为了下一轮还能算出「分数变了多少」。
     *
     * @param newScore 下一轮的分数
     * @return 下一轮的节点状态
     */
    public PageRankData next(float newScore) {
      return new PageRankData(newScore, score, urls);
    }

    /**
     * 算出本节点平均分给每个后继节点的分数
     *
     * <p>分数要平均分给出链节点，所以除数就是出链个数；
     * 本测试的数据里每个节点都有出链，不会出现除以零的情况。
     *
     * @return 分给每个后继节点的分数
     */
    public float propagatedScore() {
      return score / urls.size();
    }

    /**
     * 把节点状态渲染成一行，便于查看
     *
     * @return 包含分数、上一轮分数和出链列表的字符串
     */
    @Override
    public String toString() {
      return score + " " + lastScore + " " + urls;
    }
  }

  /**
   * 跑一轮 PageRank 计算
   *
   * <p>第一步把每个节点的分数平均分给它的所有出链节点，得到「入链节点 -&gt; 分到的分数」；
   * 第二步用 {@code cogroup} 把「节点自己的记录」和「它收到的分数」放到同一个键下，
   * 按阻尼因子加权求和，算出新的分数。
   *
   * <p>公式写成 {@code d + (1 - d) * sum}：{@code d} 是阻尼因子，
   * 代表随机跳转带来的那部分分数，{@code (1 - d) * sum} 是沿链接传过来的部分。
   *
   * @param input 上一轮的节点状态表
   * @param d 阻尼因子
   * @return 这一轮算出来的节点状态表
   */
  public PTable<String, PageRankData> pageRank(PTable<String, PageRankData> input, final float d) {
    // 第一步：把分数分给所有出链节点，得到「目标节点 -> 分到的分数」
    PTable<String, Float> outbound = input.parallelDo(new DoFn<Pair<String,
        PageRankData>, Pair<String, Float>>() {
      /**
       * 把当前节点的分数平均分给它的所有出链节点
       *
       * @param input 当前节点的状态
       * @param emitter 用来输出「目标节点 -&gt; 分到的分数」
       */
      @Override
      public void process(Pair<String, PageRankData> input, Emitter<Pair<String,
          Float>> emitter) {
        // 取出这个节点的当前状态
        PageRankData prd = input.second();
        for (String link : prd.urls) {
          // 每条出链分到同样多的分数
          emitter.emit(Pair.of(link, prd.propagatedScore()));
        }
      }
    }, tableOf(strings(), floats()));

    // 第二步：cogroup 把「自己的状态」和「收到的分数」按同一个键配到一起
    return input.cogroup(outbound).mapValues(
        new MapFn<Pair<Collection<PageRankData>, Collection<Float>>, PageRankData>() {
          /**
           * 汇总收到的分数，算出这个节点的新分数
           *
           * @param input 左边是自己的状态，右边是收到的分数
           * @return 下一轮的节点状态
           */
          @Override
          public PageRankData map(Pair<Collection<PageRankData>, Collection<Float>> input) {
            // 自己这一组里只有一个节点，就是当前节点
            PageRankData prd = Iterables.getOnlyElement(input.first());
            // 另一组是所有入链节点分过来的分数
            Collection<Float> propagatedScores = input.second();
            // 把收到的分数加起来
            float sum = 0.0f;
            for (Float s : propagatedScores) {
              sum += s;
            }
            // 按阻尼因子加权，得到新一轮的分数
            return prd.next(d + (1.0f - d) * sum);
          }
        }, input.getValueType());
  }

  /**
   * 算出所有节点里「分数变化最大」的那一个
   *
   * <p>这个值就是本轮迭代的收敛幅度：它小于阈值，就说明所有节点都基本稳定了。
   * {@code Aggregate.max} 要求元素可比较，这里算出来的正是 {@code Float}。
   *
   * @param scores 当前这一轮的节点状态表
   * @return 所有节点中分数变化绝对值的最大值
   */
  private PObject<Float> computeDelta(PTable<String, PageRankData> scores) {
    // 先算出每个节点分数变化的绝对值，再取全局最大值
    return Aggregate.max(scores.parallelDo(new MapFn<Pair<String, PageRankData>, Float>() {
      /**
       * 算出这个节点这一轮的分数变化幅度
       *
       * @param input 该节点的状态
       * @return 当前分数与上一轮分数之差的绝对值
       */
      @Override
      public Float map(Pair<String, PageRankData> input) {
        // 取出这个节点的当前状态
        PageRankData prd = input.second();
        // 当前分数与上一轮分数的差，取绝对值
        return Math.abs(prd.score - prd.lastScore);
      }
    }, floats()));
  }

  /**
   * 读入链接关系文件，得到每个节点的初始状态
   *
   * <p>urls.txt 每行是「源节点 -&gt; 目标节点」，先按源节点分组，
   * 再把每个源节点的所有出链汇总成一个 {@link PageRankData}，
   * 初始分数取 1、上一轮分数取 0。
   *
   * @param pipeline 用来读文件的流水线
   * @param urlInput 链接关系文件所在路径
   * @return 键是节点名、值是该节点状态的表
   */
  private PTable<String, PageRankData> readUrls(Pipeline pipeline, String urlInput) {
    return pipeline.readTextFile(urlInput)
        .parallelDo(new MapFn<String, Pair<String, String>>() {
          /**
           * 把一行「源节点 -&gt; 目标节点」拆成键值对
           *
           * @param input 一行文本
           * @return 由源节点和目标节点组成的键值对
           */
          @Override
          public Pair<String, String> map(String input) {
            // urls.txt 里每行用制表符分隔成源节点和目标节点
            String[] urls = input.split("\\t");
            return Pair.of(urls[0], urls[1]);
          }
        }, tableOf(strings(), strings())).groupByKey()
        .mapValues(new MapFn<Iterable<String>, PageRankData>() {
          /**
           * 把一个节点的所有出链汇总成它的初始状态
           *
           * @param input 该节点的所有出链
           * @return 初始分数为 1、上一轮分数为 0 的节点状态
           */
          @Override
          public PageRankData map(Iterable<String> input) {
            // 初始分数给 1、上一轮分数给 0，这样第一轮的变化量最大
            return new PageRankData(1.0f, 0.0f, input);
          }
        }, Avros.reflects(PageRankData.class));
  }

  /**
   * 迭代 PageRank 直到收敛，并检查最后的分数变化量
   *
   * @throws Exception 复制资源文件或执行流水线失败时抛出
   */
  @Test
  public void test() throws Exception {
    // 复制链接关系文件到临时目录
    String urlInput = tmpDir.copyResourceFileName("urls.txt");
    // MRPipeline 是基于 Hadoop MapReduce 的流水线实现
    Pipeline pipeline = new MRPipeline(getClass());
    // 读入链接关系，作为迭代的起点
    PTable<String, PageRankData> scores = readUrls(pipeline, urlInput);
    // 初值取得足够大，保证至少会跑完一轮
    Float delta = 1.0f;
    while (delta > 0.01) {
      // 算一轮新的分数
      scores = pageRank(scores, 0.5f);
      // 必须显式物化，否则这一轮的 DAG 不会真正被执行，
      // 迭代下去只会得到一条越来越长的流水线，而不是一轮轮收敛的结果
      scores.materialize().iterator(); // force scores to be materialized
      // 想调试流水线计划时，打开下面这行把计划器的 dot 图导出到 target 目录
      //writeDotFile(pipeline.getConfiguration().get("crunch.planner.dotfile"));
      // 算出这一轮全体节点中最大的分数变化
      PObject<Float> pDelta = computeDelta(scores);
      // 取值同样会触发流水线执行，并把算出来的结果缓存起来
      delta = pDelta.getValue();
      //writeDotFile(pipeline.getConfiguration().get("crunch.planner.dotfile"));
    }
    // 迭代结束，让流水线收尾并清理临时文件
    pipeline.done();
    // 收敛时的分数变化量在 0.005 上下
    assertEquals(0.0048, delta, 0.001);
  }

  /**
   * 导出 dot 图时的文件序号，避免多次导出互相覆盖
   */
  static int i = 0;

  /**
   * 异步跑一条流水线，并把它的计划图导出成 dot 文件
   *
   * <p>这里用 {@code runAsync()}，是为了先拿到执行器、把 dot 文件写出来，再等待作业结束，
   * 这样即使作业还在跑，也能先看到计划长什么样。
   *
   * @param pipeline 要执行的流水线
   * @throws Exception 导出 dot 文件或等待作业结束失败时抛出
   */
  private void run(Pipeline pipeline) throws Exception {
    // 异步提交作业，立刻拿到执行器
    PipelineExecution execution = pipeline.runAsync();
    // 把计划器的 dot 描述写进文件，方便用 Graphviz 之类的工具查看
    writeDotFile(execution.getPlanDotFile());
    // 阻塞等待作业真正跑完
    execution.waitUntilDone();
  }

  /**
   * 把 dot 文件的内容写到 target 目录下带序号的文件里
   *
   * @param dotfile dot 文件的内容
   * @throws Exception 写文件失败时抛出
   */
  private void writeDotFile(String dotfile) throws Exception {
    // 文件名里带上递增的序号，多次导出不会互相覆盖
    FileUtils.write(new File("target/pagerank-" + i++ + ".dot"), dotfile, "UTF-8");
  }
}
