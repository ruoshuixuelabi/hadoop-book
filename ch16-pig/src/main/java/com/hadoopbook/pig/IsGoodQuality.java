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
 * 过滤掉温度质量码不合格的记录
 *
 * <p>NCDC 的每条记录都带一个「质量码」，
 * 表示这条温度读数有多可信。
 * 这里只保留质量码为 0、1、4、5、9 的记录，
 * 其余的一律丢弃
 * <p>这是一个 {@code FilterFunc}：
 * 返回 {@code true} 表示保留这条记录，
 * 返回 {@code false} 表示丢掉它
 */
public class IsGoodQuality extends FilterFunc {

  /**
   * 判断一条记录的质量码是否合格
   *
   * @param tuple 输入元组，至少有一个整数类型的质量码字段
   * @return 合格返回 {@code true}，否则返回 {@code false}
   * @throws IOException 读取字段失败
   */
  @Override
  public Boolean exec(Tuple tuple) throws IOException {
    // 没有数据，或者这一行没有任何字段
    if (tuple == null || tuple.size() == 0) {
      return false;
    }
    try {
      // 只看第一个字段
      Object object = tuple.get(0);
      // 字段是空的，没法判断
      if (object == null) {
        return false;
      }
      // 质量码是一个整数
      int i = (Integer) object;
      // 0、1、4、5、9 这几种质量码算合格
      return i == 0 || i == 1 || i == 4 || i == 5 || i == 9;
    } catch (ExecException e) {
      // Pig 的异常包一层再抛出去
      throw new IOException(e);
    }
  }
//^^ IsGoodQuality
//vv IsGoodQualityTyped
  /**
   * 告诉 Pig 这个 UDF 接收什么参数
   *
   * @return 参数类型描述列表
   * @throws FrontendException 生成描述失败
   */
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    // 参数类型描述
    List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
    // 只有一个参数：整数类型的质量码
    funcSpecs.add(new FuncSpec(this.getClass().getName(),
        new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));

    // 把描述返回给 Pig
    return funcSpecs;
  }
//^^ IsGoodQualityTyped  
//vv IsGoodQuality  
}
// ^^ IsGoodQuality