package com.hadoopbook.pig;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.pig.data.*;
import org.junit.*;

/**
 * IsGoodQuality UDF的单元测试类。
 *
 * <p>该测试类验证IsGoodQuality过滤函数对各种输入的处理是否正确。
 * 测试覆盖了null输入、空Tuple、null字段以及各种质量代码值的场景。</p>
 *
 * <p>测试策略：
 * <ul>
 *   <li>边界条件测试：null Tuple、空Tuple、null字段</li>
 *   <li>坏质量代码测试：应返回false的代码（如2）</li>
 *   <li>好质量代码测试：应返回true的代码（如0, 1, 4, 5, 9）</li>
 * </ul>
 * </p>
 *
 * <p>根据NCDC气象数据标准：
 * <ul>
 *   <li>好质量：0, 1, 4, 5, 9</li>
 *   <li>坏质量：2, 3, 6, 7, 8等</li>
 * </ul>
 * </p>
 *
 * @see IsGoodQuality
 */
public class IsGoodQualityTest {

    /** 待测试的IsGoodQuality实例 */
    private IsGoodQuality func;

    /**
     * 测试前置方法，创建IsGoodQuality实例。
     *
     * <p>每个测试方法执行前都会调用此方法，初始化测试所需的UDF实例。</p>
     */
    @Before
    public void setUp() {
      func = new IsGoodQuality();
    }

    /**
     * 测试null Tuple输入的处理。
     *
     * <p>验证当传入null Tuple时，exec方法返回false，过滤掉该记录。</p>
     *
     * @throws IOException 测试不抛出异常
     */
    @Test
    public void nullTuple() throws IOException {
      assertThat(func.exec(null), is(false));
    }

    /**
     * 测试空Tuple输入的处理。
     *
     * <p>验证当传入空Tuple（即没有任何字段）时，exec方法返回false。</p>
     *
     * @throws IOException 测试不抛出异常
     */
    @Test
    public void emptyTuple() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple();
      assertThat(func.exec(tuple), is(false));
    }

    /**
     * 测试包含null字段的Tuple的处理。
     *
     * <p>验证当Tuple的第一个字段为null时（表示数据缺失），
     * exec方法返回false，过滤掉该记录。</p>
     *
     * @throws IOException 测试不抛出异常
     */
    @Test
    public void tupleWithNullField() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple((Object) null);
      assertThat(func.exec(tuple), is(false));
    }

    /**
     * 测试坏质量代码（值为2）的处理。
     *
     * <p>验证当质量代码为2时（表示数据可疑），exec方法返回false，
     * 该记录应被过滤掉。</p>
     *
     * @throws IOException 测试不抛出异常
     */
    @Test
    public void badQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(2));
      assertThat(func.exec(tuple), is(false));
    }

    /**
     * 测试好质量代码（值为1）的处理。
     *
     * <p>验证当质量代码为1时（表示质量良好），exec方法返回true，
     * 该记录应被保留。好质量代码包括：0, 1, 4, 5, 9。</p>
     *
     * @throws IOException 测试不抛出异常
     */
    @Test
    public void goodQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(1));
      assertThat(func.exec(tuple), is(true));
    }


}
