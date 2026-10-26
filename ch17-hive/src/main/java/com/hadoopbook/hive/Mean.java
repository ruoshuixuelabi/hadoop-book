package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * Hive 聚集函数：求一列的平均值
 *
 * <p>平均值不能像最大值那样「边看边合并」，
 * 因为除法不满足结合律：
 * 所以这里分成两个阶段，
 * 先在各个分组里算出「总和 + 次数」，
 * 等所有分组都汇总完毕，再做一次除法
 */
public class Mean extends UDAF {

  /**
   * 真正实现求平均值逻辑的求值器
   *
   * <p>它多了一个 {@link PartialResult} 用来携带中间结果，
   * 这样各分组的「总和 + 次数」才能被合并
   */
  public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
    /**
     * 部分结果：一段数据的总和与条数
     */
    public static class PartialResult {
      // 累加的总和
      double sum;
      // 参与计算的数据条数
      long count;
    }
    
    // 当前累计的部分结果；还没有数据时是 null
    private PartialResult partial;

    /**
     * 开始处理一个新的分组
     */
    public void init() {
      // 每个分组都要从头开始累计
      partial = null;
    }

    /**
     * 处理一行数据，累加总和与次数
     *
     * @param value 这一行的值
     * @return 恒为 {@code true}，表示这一行处理成功
     */
    public boolean iterate(DoubleWritable value) {
      // 空值直接跳过，但仍然算处理成功
      if (value == null) {
        return true;
      }
      // 第一条数据到来时才建对象
      if (partial == null) {
        // 新建一个空的部分结果
        partial = new PartialResult();
      }
      // 累加总和
      partial.sum += value.get();
      // 累加条数
      partial.count++;
      return true;
    }

    /**
     * 交出目前累计出来的部分结果
     *
     * @return 累计到的总和与条数
     */
    public PartialResult terminatePartial() {
      // 把部分结果交出去，等待与其他分组合并
      return partial;
    }

    /**
     * 把另一个分组的部分结果合并进来
     *
     * @param other 别的分组交出来的部分结果
     * @return 恒为 {@code true}
     */
    public boolean merge(PartialResult other) {
      // 对方没有数据，不用合并
      if (other == null) {
        return true;
      }
      // 自己还没有数据，先把累加器建出来
      if (partial == null) {
        // 新建一个空的部分结果
        partial = new PartialResult();
      }
      // 总和相加
      partial.sum += other.sum;
      // 条数相加
      partial.count += other.count;
      return true;
    }

    /**
     * 给出最终结果
     *
     * @return 平均值；一条数据都没有时返回 {@code null}
     */
    public DoubleWritable terminate() {
      // 一条数据都没有，算不出平均值
      if (partial == null) {
        return null;
      }
      // 全部合并完之后再除，得到平均值
      return new DoubleWritable(partial.sum / partial.count);
    }
  }
}
