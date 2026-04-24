package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * 计算整型最大值的自定义聚合函数（UDAF）。
 *
 * <p>此类实现了Hive的用户自定义聚合函数（User Defined Aggregate Function），
 * 用于计算一组整数中的最大值。它遵循MapReduce的combine-reduce模式，
 * 支持部分聚合（partial aggregation），能够显著提升大规模数据集的处理效率。
 *
 * <p>聚合流程：
 * <ol>
 *   <li>{@link #iterate(IntWritable)} - 处理每个输入值，更新当前最大值</li>
 *   <li>{@link #terminatePartial()} - 返回当前节点的部分结果（用于combine阶段）</li>
 *   <li>{@link #merge(PartialResult)} - 合并来自其他节点的PartialResult</li>
 *   <li>{@link #terminate()} - 返回最终的最大值结果</li>
 * </ol>
 *
 * <p>示例Hive查询：
 * <pre>
 * CREATE TEMPORARY FUNCTION maximum AS 'com.hadoopbook.hive.Maximum';
 * SELECT maximum(temperature) FROM records WHERE temperature != 9999;
 * </pre>
 *
 * @see UDAF
 * @see UDAFEvaluator
 */
public class Maximum extends UDAF {

    /**
     * 最大值聚合函数的求值器实现类。
     *
     * <p>内部类实现完整的UDAF逻辑，支持部分聚合以优化性能。
     * 通过{@link #iterate(IntWritable)}处理每个输入值，
     * 通过{@link #merge(IntWritable)}合并多个mapper/reducer的结果。
     */
    public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {

        /**
         * 当前已处理的最大值。
         * 初始为null，表示尚未处理任何有效值。
         */
        private IntWritable result;

        /**
         * 初始化求值器。
         * 在每次聚合开始时被调用，重置内部状态为初始值。
         *
         * <p>调试信息输出当前实例的hashCode和操作名称，
         * 用于追踪多线程或分布式环境中的调用情况。
         */
        public void init() {
            System.err.printf("%s %s\n", hashCode(), "init");
            result = null;
        }

        /**
         * 处理一个输入值，更新当前最大值。
         *
         * <p>此方法在MapReduce的map阶段被调用，用于处理每条记录。
         * 如果输入值为null或空值，直接返回true跳过处理。
         * 如果result尚未初始化（为null），则用当前值创建新的IntWritable。
         * 否则比较当前值与已有最大值，取较大者更新。
         *
         * @param value 待处理的整型输入值，不允许为null
         * @return 处理成功返回true，始终返回true（null值被视为有效处理）
         */
        public boolean iterate(IntWritable value) {
            System.err.printf("%s %s %s\n", hashCode(), "iterate", value);
            // null值视为有效处理，直接跳过，避免空指针异常
            if (value == null) {
                return true;
            }
            // 首次遇到有效值时，初始化result为当前值
            if (result == null) {
                result = new IntWritable(value.get());
            }
            else {
                // 比较并更新最大值：当前值 vs 已记录的最大值
                result.set(Math.max(result.get(), value.get()));
            }
            return true;
        }

        /**
         * 返回当前节点的部分聚合结果。
         *
         * <p>此方法在MapReduce的combine阶段被调用，
         * 将当前节点的处理结果（当前最大值）发送给下一个Reducer进行合并。
         *
         * @return 当前节点的最大值结果；如果尚未处理任何有效值则返回null
         */
        public IntWritable terminatePartial() {
            System.err.printf("%s %s\n", hashCode(), "terminatePartial");
            return result;
        }

        /**
         * 合并来自其他节点的部分结果。
         *
         * <p>此方法在Reduce阶段被调用，接收来自不同mapper/combiner的
         * 部分结果并合并。由于本实现中部分结果就是当前最大值，
         * 因此直接调用{@link #iterate(IntWritable)}来处理合并逻辑。
         *
         * <p>这种设计确保了合并逻辑与处理新输入值的逻辑完全一致，
         * 避免了代码重复和潜在的逻辑不一致问题。
         *
         * @param other 来自其他节点的部分结果
         * @return 合并成功返回true，始终返回true（null值视为有效合并）
         */
        public boolean merge(IntWritable other) {
            System.err.printf("%s %s %s\n", hashCode(), "merge", other);
            // 直接委托给iterate处理：other被视为新输入值参与最大值的比较
            return iterate(other);
        }

        /**
         * 返回最终的聚合结果。
         *
         * <p>此方法在所有数据处理完成后被调用，返回计算出的最大值。
         * 如果从未处理过任何有效值（即result仍为null），则返回null。
         *
         * @return 最终的最大值；如果无有效输入则返回null
         */
        public IntWritable terminate() {
            System.err.printf("%s %s\n", hashCode(), "terminate");
            return result;
        }
    }
}