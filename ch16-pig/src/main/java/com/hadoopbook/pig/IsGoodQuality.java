//cc IsGoodQuality A FilterFunc UDF to remove records with unsatisfactory temperature quality readings
// == IsGoodQualityTyped
//vv IsGoodQuality
package com.hadoopbook.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FilterFunc;
//^^ IsGoodQuality
import org.apache.pig.FuncSpec;
//vv IsGoodQuality
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
//^^ IsGoodQuality
import org.apache.pig.impl.logicalLayer.schema.Schema;
//vv IsGoodQuality

/**
 * IsGoodQuality是一个Pig FilterFunc UDF，用于过滤掉质量不佳的气温记录。
 *
 * <p>在气象数据中，每条记录都包含一个质量代码，用于指示该记录数据的可靠性。
 * 本过滤器用于保留"好"质量的记录，过滤掉"坏"质量的记录。</p>
 *
 * <p>根据气象数据标准（NCDC NOAA格式），质量代码含义如下：
 * <ul>
 *   <li><b>0</b> - 质量码通过，质量良好</li>
 *   <li><b>1</b> - 质量码通过，保留</li>
 *   <li><b>2</b> - 质量码未通过，数据可疑</li>
 *   <li><b>4</b> - 质量码通过，保留（用于温度）</li>
 *   <li><b>5</b> - 质量码通过，保留（用于温度）</li>
 *   <li><b>9</b> - 缺失质量码</li>
 * </ul>
 * </p>
 *
 * <p>好质量代码：0, 1, 4, 5, 9（这些代码表示数据是可用的）
 * 坏质量代码：2, 3, 6, 7, 8等（这些代码表示数据有问题）</p>
 *
 * <p>使用示例（Pig Latin）：
 * <pre>
 * raw = LOAD 'ncdc_records' AS (year:chararray, temperature:int, quality:int);
 * filtered = FILTER raw BY com.hadoopbook.pig.IsGoodQuality(quality);
 * </pre>
 *
 * <p>该UDF通常与STREAM操作配合使用，对数据进行质量过滤。</p>
 *
 * @see FilterFunc
 * @see Tuple
 */
public class IsGoodQuality extends FilterFunc {

    /**
     * 执行过滤判断。
     *
     * <p>此方法检查给定的Tuple是否应该保留在数据集中。
     * 只有质量代码为好代码（0, 1, 4, 5, 9）的记录才会被保留。</p>
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>如果Tuple为null或为空，返回false（过滤掉）</li>
     *   <li>如果Tuple的第一个字段为null，返回false（过滤掉）</li>
     *   <li>获取质量代码值，检查是否为有效的好质量代码</li>
     * </ol>
     * </p>
     *
     * @param tuple 包含数据的Tuple，通常第一个元素是质量代码（Integer）
     * @return true表示记录质量良好应保留，false表示应过滤
     * @throws IOException 如果读取Tuple字段失败
     */
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        // null Tuple或空Tuple直接过滤
        if (tuple == null || tuple.size() == 0) {
            return false;
        }
        try {
            // 获取第一个字段（质量代码）
            Object object = tuple.get(0);
            // null字段表示数据缺失，过滤掉
            if (object == null) {
                return false;
            }
            // 质量代码必须是Integer类型
            int i = (Integer) object;
            // 判断是否为有效的好质量代码：0, 1, 4, 5, 9
            return i == 0 || i == 1 || i == 4 || i == 5 || i == 9;
        }
        catch (ExecException e) {
            // 将ExecException转换为IOException，保持接口声明
            throw new IOException(e);
        }
    }

    //^^ IsGoodQuality
//vv IsGoodQualityTyped

    /**
     * 获取参数到函数的类型映射。
     *
     * <p>此方法定义了UDF参数的类型签名，让Pig能够进行类型检查和类型转换。
     * 本方法声明该UDF接受一个INTEGER类型的参数。</p>
     *
     * <p>在Pig执行过程中，会根据此映射验证传入参数的类型是否匹配。
     * 如果不匹配，Pig会尝试进行类型转换（如将chararray转为integer）。</p>
     *
     * @return FuncSpec列表，描述参数类型信息
     * @throws FrontendException 如果创建类型映射失败
     */
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
        // 创建FuncSpec，指定参数类型为INTEGER
        funcSpecs.add(new FuncSpec(this.getClass().getName(),
                new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));

        return funcSpecs;
    }
//^^ IsGoodQualityTyped
//vv IsGoodQuality
}
// ^^ IsGoodQuality
