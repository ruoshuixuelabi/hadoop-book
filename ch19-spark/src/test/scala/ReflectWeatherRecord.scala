// 这里介绍的是 Avro 的第三种数据模型「反射（reflect）」：
// 不生成专门的 Java 类，而是直接拿一个普通的 Scala 类当数据结构，
// 读写时靠反射按字段名对号入座。
case class ReflectWeatherRecord(year: Int, temperature: Int, stationId: String) {
  // 因为 Avro 在反序列化时要凭空造出一个空对象再逐字段填值，
  // 所以这个类必须提供一个无参构造函数；
  // 而 case class 自动生成的只有带参构造，这里手写一个补上。
  def this() = this(0, 0, null)
}