package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * Hive 聚集函数：求一列的最大值
 *
 * <p>{@link UDAF} 是 Hive 的「聚集函数」基类：
 * 与普通 UDF 一次只看一行不同，
 * 聚集函数会收到很多行，边收边算，最后给出一个结果
 * <p>实际干活的类是里面的 {@code UDAFEvaluator}，
 * Hive 会为每个分组创建一个实例
 */
public class Maximum extends UDAF {

  /**
   * 真正实现求最大值逻辑的求值器
   *
   * <p>{@code UDAFEvaluator} 定义了聚集函数的生命周期：
   * <ul>
   * <li>{@code init}：开始一个新分组</li>
   * <li>{@code iterate}：来一行数据</li>
   * <li>{@code terminatePartial}：收尾，交出部分结果</li>
   * <li>{@code merge}：把别的分组的部分结果合进来</li>
   * <li>{@code terminate}：给出最终结果</li>
   * </ul>
   */
  public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
    
    // 当前见过的最大值；还没有数据时是 null
    private IntWritable result;
    
    /**
     * 开始处理一个新的分组
     */
    public void init() {
      // 调试输出：打印实例编号和走到哪个生命周期方法
      System.err.printf("%s %s\n", hashCode(), "init");
      // 每个分组都要从头开始累计
      result = null;
    }

    /**
     * 处理一行数据，更新当前最大值
     *
     * @param value 这一行的值
     * @return 恒为 {@code true}，表示这一行处理成功
     */
    public boolean iterate(IntWritable value) {
      // 调试输出：连这一行的值一起打出来
      System.err.printf("%s %s %s\n", hashCode(), "iterate", value);
      // 空值直接跳过，但仍然算处理成功
      if (value == null) {
        return true;
      }
      // 还没有见过任何数据，先把这一条记下来
      if (result == null) {
        // 第一个值就是当前最大值
        result = new IntWritable(value.get());
      } else {
        // 已经有最大值了，取更大的那个
        result.set(Math.max(result.get(), value.get()));
      }
      return true;
    }

    /**
     * 交出目前累计出来的部分结果
     *
     * @return 当前的最大值
     */
    public IntWritable terminatePartial() {
      // 调试输出
      System.err.printf("%s %s\n", hashCode(), "terminatePartial");
      // 把当前的部分结果交出去
      return result;
    }

    /**
     * 把另一个分组的部分结果合并进来
     *
     * @param other 别的分组交出来的部分结果
     * @return 恒为 {@code true}
     */
    public boolean merge(IntWritable other) {
      // 调试输出
      System.err.printf("%s %s %s\n", hashCode(), "merge", other);
      // 合并一个最大值，和处理一行数据是同一套逻辑
      return iterate(other);
    }

    /**
     * 给出最终结果
     *
     * @return 这一组的最大值
     */
    public IntWritable terminate() {
      // 调试输出
      System.err.printf("%s %s\n", hashCode(), "terminate");
      // 最终结果
      return result;
    }
  }
}
