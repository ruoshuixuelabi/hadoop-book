package com.hadoopbook.pig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.pig.data.*;
import org.junit.*;

/**
 * {@link IsGoodQuality} 的测试
 *
 * <p>覆盖了元组为 null、元组为空、字段为 null
 * 以及质量码合格与不合格等各种情况
 */
public class IsGoodQualityTest {
    
    // 被测的 UDF
    private IsGoodQuality func;
    
    /**
     * 每个用例开始前都新建一个 UDF
     */
    @Before
    public void setUp() {
      // UDF 本身没有状态，新建一个即可
      func = new IsGoodQuality();
    }

    /**
     * 传入 null 元组时返回 false
     */
    @Test
    public void nullTuple() throws IOException {
      // null 元组应当被判定为不合格
      assertThat(func.exec(null), is(false));
    }
    
    /**
     * 传入空元组时返回 false
     */
    @Test
    public void emptyTuple() throws IOException {
      // 空元组：没有任何字段
      Tuple tuple = TupleFactory.getInstance().newTuple();
      // 没有质量码，判定为不合格
      assertThat(func.exec(tuple), is(false));
    }

    /**
     * 字段为 null 时返回 false
     */
    @Test
    public void tupleWithNullField() throws IOException {
      // 只有一个字段，而且字段是 null
      Tuple tuple = TupleFactory.getInstance().newTuple((Object) null);
      // 质量码缺失，判定为不合格
      assertThat(func.exec(tuple), is(false));
    }
    
    /**
     * 质量码不合格时返回 false
     */
    @Test
    public void badQuality() throws IOException {
      // 质量码 2 属于不合格
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(2));
      // 应当被过滤掉
      assertThat(func.exec(tuple), is(false));
    }
    
    /**
     * 质量码合格时返回 true
     */
    @Test
    public void goodQuality() throws IOException {
      // 质量码 1 属于合格
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(1));
      // 应当被保留下来
      assertThat(func.exec(tuple), is(true));
    }
    

}
