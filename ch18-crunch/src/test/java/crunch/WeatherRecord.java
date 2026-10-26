package crunch;

/**
 * 天气观测记录：一个简单的可序列化值对象
 *
 * <p>Crunch 的 PCollection 元素需要能被序列化，
 * 所以这类「记录类」通常写成扁平的 POJO：私有字段 + 无参构造 + getter。
 * <p>实现 equals/hashCode 是必须的：
 * Map/Reduce 的分组与去重都靠它们判断两条记录是不是同一条，
 * 否则同一气象站不同年份的记录可能被错误地当成不同的键。
 */
public class WeatherRecord {
  // 观测年份，通常被当作分组时的键
  private int year;
  // 观测温度，通常是被聚合、被排序的那个度量值
  private int temperature;
  // 气象站编号，用来区分不同的观测站
  private String stationId;

  /**
   * 无参构造函数：序列化框架（如 Avro）反序列化时
   * 需要先造一个空对象再逐字段赋值
   */
  public WeatherRecord() {
  }

  /**
   * 构造一条完整的观测记录
   *
   * @param year 观测年份
   * @param temperature 观测温度
   * @param stationId 气象站编号
   */
  public WeatherRecord(int year, int temperature, String stationId) {
    // 逐个字段赋值：先年份，再温度，最后站号
    this.year = year;
    this.temperature = temperature;
    this.stationId = stationId;
  }

  // ... getters elided

  /**
   * 取观测年份
   *
   * @return 年份
   */
  public int getYear() {
    return year;
  }

  /**
   * 取观测温度
   *
   * @return 温度
   */
  public int getTemperature() {
    return temperature;
  }

  /**
   * 取气象站编号
   *
   * @return 气象站编号
   */
  public String getStationId() {
    return stationId;
  }

  /**
   * 按三个字段判断两条记录是否代表同一次观测
   *
   * @param o 待比较的对象
   * @return 三个字段都相等时返回 {@code true}
   */
  @Override
  public boolean equals(Object o) {
    // 快速路径：同一个引用必然相等
    if (this == o) return true;
    // 类型不同就不可能相等；这里用 getClass() 而不是 instanceof，
    // 这样子类实例不会被当成父类实例
    if (o == null || getClass() != o.getClass()) return false;

    // 类型已经确认一致，这里可以安全地强转
    WeatherRecord that = (WeatherRecord) o;

    if (temperature != that.temperature) return false;
    if (year != that.year) return false;
    // stationId 是引用类型，可能为 null，
    // 所以要分清「两边都是 null」和「只有一边是 null」两种情况
    if (stationId != null ? !stationId.equals(that.stationId) : that.stationId != null)
      return false;

    return true;
  }

  /**
   * 计算哈希值，必须与 {@link #equals(Object)} 保持一致
   *
   * @return 由三个字段算出的哈希值
   */
  @Override
  public int hashCode() {
    // 用 31 这个素数做乘法散列，能把高位的信息带到低位，减少碰撞
    int result = year;
    result = 31 * result + temperature;
    // 引用字段可能为 null，用 0 兜底，保证哈希值始终非负且稳定
    result = 31 * result + (stationId != null ? stationId.hashCode() : 0);
    return result;
  }

  /**
   * 转换成便于阅读的字符串，调试输出时很有用
   *
   * @return 形如 {@code WeatherRecord{year=2010, temperature=35, ...}} 的字符串
   */
  @Override
  public String toString() {
    // 字符串拼接虽然啰嗦，但只在调试时被调用，性能不敏感
    return "WeatherRecord{" +
        "year=" + year +
        ", temperature=" + temperature +
        ", stationId='" + stationId + '\'' +
        '}';
  }
}
