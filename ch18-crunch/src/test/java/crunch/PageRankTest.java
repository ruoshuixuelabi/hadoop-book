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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.crunch.types.avro.Avros.floats;
import static org.apache.crunch.types.avro.Avros.strings;
import static org.apache.crunch.types.avro.Avros.tableOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PageRank算法测试
 *
 * 演示如何在Crunch中实现迭代算法:
 * - 读取URL图数据
 * - 迭代计算PageRank直到收敛
 * - 使用materialize()强制物化中间结果
 *
 * PageRank算法:
 * PR(A) = (1-d)/N + d * sum(PR(T)/C(T))
 * - d: 阻尼因子(通常0.85)
 * - N: 总页面数
 * - PR(T): 指向A的页面T的PageRank
 * - C(T): 页面T的出链接数
 */
public class PageRankTest implements Serializable {

    /** 临时目录规则 */
    public transient Path tmpDir;

    /**
     * PageRank数据结构
     *
     * 包含当前分数、上次分数和出链接列表
     */
    public static class PageRankData {
        public float score; // 当前PageRank分数
        public float lastScore; // 上次迭代的分数
        public List<String> urls; // 出链接列表

        public PageRankData() {
        }

        public PageRankData(float score, float lastScore, Iterable<String> urls) {
            this.score = score;
            this.lastScore = lastScore;
            this.urls = Lists.newArrayList(urls);
        }

        /**
         * 创建下一迭代的PageRankData
         *
         * @param newScore 新的PageRank分数
         * @return 新的PageRankData
         */
        public PageRankData next(float newScore) {
            return new PageRankData(newScore, score, urls);
        }

        /**
         * 计算传播分数
         *
         * @return 当前分数除以出链接数
         */
        public float propagatedScore() {
            return score / urls.size();
        }

        @Override
        public String toString() {
            return score + " " + lastScore + " " + urls;
        }
    }

    /**
     * 执行一次PageRank迭代
     *
     * @param input 当前迭代的PageRank数据
     * @param d 阻尼因子
     * @return 下一迭代的PageRank数据
     */
    public PTable<String, PageRankData> pageRank(PTable<String, PageRankData> input, final float d) {
        // 计算每个URL从其入链获得的分数
        PTable<String, Float> outbound = input.parallelDo(new DoFn<Pair<String,
            PageRankData>, Pair<String, Float>>() {
            @Override
            public void process(Pair<String, PageRankData> input, Emitter<Pair<String,
                Float>> emitter) {
                PageRankData prd = input.second();
                for (String link : prd.urls) {
                    // 将当前分数按出链接数平均分配
                    emitter.emit(Pair.of(link, prd.propagatedScore()));
                }
            }
        }, tableOf(strings(), floats()));

        // 合并当前分数和传播分数,计算新的PageRank
        return input.cogroup(outbound).mapValues(
            new MapFn<Pair<Collection<PageRankData>, Collection<Float>>, PageRankData>() {
                @Override
                public PageRankData map(Pair<Collection<PageRankData>, Collection<Float>> input) {
                    // 获取当前的PageRankData
                    PageRankData prd = Iterables.getOnlyElement(input.first());
                    // 计算传播分数的总和
                    Collection<Float> propagatedScores = input.second();
                    float sum = 0.0f;
                    for (Float s : propagatedScores) {
                        sum += s;
                    }
                    // 计算新的PageRank: (1-d) + d * sum
                    return prd.next(d + (1.0f - d) * sum);
                }
            }, input.getValueType());
    }

    /**
     * 计算两次迭代之间的分数变化
     *
     * @param scores PageRank数据表
     * @return 变化量
     */
    private PObject<Float> computeDelta(PTable<String, PageRankData> scores) {
        return Aggregate.max(scores.parallelDo(new MapFn<Pair<String, PageRankData>, Float>() {
            @Override
            public Float map(Pair<String, PageRankData> input) {
                PageRankData prd = input.second();
                return Math.abs(prd.score - prd.lastScore);
            }
        }, floats()));
    }

    /**
     * 从文件读取URL图数据
     *
     * @param pipeline Pipeline实例
     * @param urlInput URL文件路径
     * @return PageRank数据表
     */
    private PTable<String, PageRankData> readUrls(Pipeline pipeline, String urlInput) {
        return pipeline.readTextFile(urlInput)
            // 解析URL对
            .parallelDo(new MapFn<String, Pair<String, String>>() {
                @Override
                public Pair<String, String> map(String input) {
                    String[] urls = input.split("\t");
                    return Pair.of(urls[0], urls[1]);
                }
            }, tableOf(strings(), strings()))
            // 按源URL分组
            .groupByKey()
            // 创建初始PageRank数据(初始分数1.0)
            .mapValues(new MapFn<Iterable<String>, PageRankData>() {
                @Override
                public PageRankData map(Iterable<String> input) {
                    return new PageRankData(1.0f, 0.0f, input);
                }
            }, Avros.reflects(PageRankData.class));
    }

    /**
     * 测试PageRank算法
     *
     * 迭代计算直到收敛(分数变化<0.01)
     */
    @Test
    public void test() throws Exception {
        String urlInput = tmpDir.copyResourceFileName("urls.txt");
        Pipeline pipeline = new MRPipeline(getClass());
        PTable<String, PageRankData> scores = readUrls(pipeline, urlInput);

        // 迭代计算PageRank
        Float delta = 1.0f;
        while (delta > 0.01) {
            scores = pageRank(scores, 0.5f);
            // 强制物化,否则迭代无法收敛
            scores.materialize().iterator();
            // 计算分数变化
            PObject<Float> pDelta = computeDelta(scores);
            delta = pDelta.getValue();
        }
        pipeline.done();
        assertEquals(0.0048, delta, 0.001);
    }

    static int i = 0;

    /**
     * 运行管道并生成DOT文件
     *
     * @param pipeline Pipeline实例
     */
    private void run(Pipeline pipeline) throws Exception {
        PipelineExecution execution = pipeline.runAsync();
        writeDotFile(execution.getPlanDotFile());
        execution.waitUntilDone();
    }

    /**
     * 将DOT文件写入磁盘
     *
     * @param dotfile DOT格式的执行计划
     */
    private void writeDotFile(String dotfile) throws Exception {
        FileUtils.write(new File("target/pagerank-" + i++ + ".dot"), dotfile, "UTF-8");
    }
}