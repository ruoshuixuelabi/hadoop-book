package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

/**
 * 计算数值平均值的自定义聚合函数（UDAF）。
 *
 * <p>此类实现了Hive的用户自定义聚合函数（User Defined Aggregate Function），
 * 用于计算一组Double类型数值的平均值（算术平均数）。
 *
 * <p>与{@link Maximum}不同，平均值计算需要保存两个状态信息：
 * <ul>
 *   <li>sum - 所有数值的累加和</li>
 *   <li>count - 已处理的数值个数</li>
 * </ul>
 * 最终结果为 sum / count。
 *
 * <p>这种设计使得函数能够在MapReduce框架中正确地进行部分聚合：
 * 每个mapper/combiner先计算本地数据的平均值（以sum和count形式返回），
 * reducer再将这些部分结果合并得到最终的全局平均值。
 *
 * <p>示例Hive查询：
 * <pre>
 * CREATE TEMPORARY FUNCTION mean AS 'com.hadoopbook.hive.Mean';
 * SELECT mean(salary) FROM employees;
 * </pre>
 *
 * @see UDAF
 * @see UDAFEvaluator
 */
public class Mean extends UDAF {

    /**
     * 平均值聚合函数的求值器实现类。
     *
     * <p>内部类维护两个状态变量：sum（累加和）和count（计数），
     * 用于在部分聚合和最终合并时正确计算平均值。
     */
    public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {

        /**
         * 部分聚合结果的数据结构。
         * 包含累加和(sum)和数值个数(count)两个字段。
         *
         * <p>这种设计用于在combiner阶段传递中间结果，
         * 而不是直接传递平均值，避免多次除法带来的精度损失。
         */
        public static class PartialResult {
            /** 所有数值的累加和 */
            double sum;
            /** 已处理的数值个数 */
            long count;
        }

        /**
         * 当前节点的部分聚合结果。
         * 初始为null，在首次处理有效值时创建。
         */
        private PartialResult partial;

        /**
         * 初始化求值器。
         * 重置内部状态，为新的聚合操作做准备。
         */
        public void init() {
            partial = null;
        }

        /**
         * 处理一个输入值，累加到部分聚合结果中。
         *
         * <p>此方法在map阶段被调用，处理每条输入记录。
         * 如果输入值为null，直接返回true跳过处理。
         * 如果部分结果尚未初始化，则创建新的PartialResult。
         * 然后将当前值累加到sum中，并增加计数。
         *
         * @param value 待处理的Double类型输入值
         * @return 处理成功返回true，始终返回true（null值被视为有效处理）
         */
        public boolean iterate(DoubleWritable value) {
            // null值视为有效处理，直接跳过
            if (value == null) {
                return true;
            }
            // 首次处理有效值时，初始化PartialResult
            if (partial == null) {
                partial = new PartialResult();
            }
            // 累加当前值到sum中，并增加计数
            partial.sum += value.get();
            partial.count++;
            return true;
        }

        /**
         * 返回当前节点的部分聚合结果。
         *
         * <p>此方法在combiner阶段被调用，
         * 返回包含sum和count的结构体，供后续reduce阶段合并使用。
         *
         * @return 当前节点的部分聚合结果；如果尚未处理任何有效值则返回null
         */
        public PartialResult terminatePartial() {
            return partial;
        }

        /**
         * 合并来自其他节点的部分聚合结果。
         *
         * <p>此方法在reduce阶段被调用，接收来自不同mapper/combiner的
         * 部分结果并合并。合并方式为将各自的sum和count分别累加。
         *
         * @param other 来自其他节点的部分结果
         * @return 合并成功返回true，始终返回true（null值视为有效合并）
         */
        public boolean merge(PartialResult other) {
            // null值视为有效合并，直接跳过
            if (other == null) {
                return true;
            }
            // 确保本地partial已初始化
            if (partial == null) {
                partial = new PartialResult();
            }
            // 累加来自其他节点的sum和count到本地结果
            partial.sum += other.sum;
            partial.count += other.count;
            return true;
        }

        /**
         * 返回最终的聚合结果（平均值）。
         *
         * <p>此方法在所有数据处理完成后被调用。
         * 如果从未处理过任何有效值（partial为null），则返回null。
         * 否则返回 sum / count 计算得到的平均值。
         *
         * @return 最终的平均值；如果无有效输入则返回null
         */
        public DoubleWritable terminate() {
            if (partial == null) {
                return null;
            }
            // 计算算术平均值：sum / count
            return new DoubleWritable(partial.sum / partial.count);
        }
    }
}