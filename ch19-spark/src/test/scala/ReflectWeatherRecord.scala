/**
 * 文件：ReflectWeatherRecord.scala<br>
 * 路径：src/test/scala/ReflectWeatherRecord.scala<br>
 * 描述：用于Avro Reflect模式的天气记录POJO类<br>
 *<br>
 * 功能说明：<br>
 * 这是一个普通的Scala case class，用于Avro的Reflect数据模型。<br>
 * Avro Reflect模式通过Java反射机制将POJO字段映射到Avro schema，<br>
 * 无需预编译的schema或代码生成，适合灵活的场景。<br>
 *<br>
 * 使用场景：<br>
 * 通常与ReflectDatumWriter和AvroKeyInputFormat配合使用，<br>
 * 实现自定义POJO与Avro二进制格式之间的序列化/反序列化。<br>
 *
 * @param year 年份，气象数据的年份
 * @param temperature 温度，单位为十分之一摄氏度
 * @param stationId 气象站ID，标识数据来源
 */
case class ReflectWeatherRecord(year: Int, temperature: Int, stationId: String) {
  /**
   * 无参构造函数<br>
   * Avro ReflectDatumWriter在反序列化时需要通过反射调用无参构造器创建实例，<br>
   * 然后再通过反射设置各个字段的值。<br>
   * Scala的case class默认不生成无参构造器，因此需要显式定义。<br>
   */
  def this() = this(0, 0, null)
}
