// 这个类演示如何给 Spark 换用 Kryo 序列化器。
// 默认的 Java 序列化器来自 JDK，它要求被序列化的类实现 java.io.Serializable；
// 而 Kryo 则不挑这个，它默认用反射按字段读写对象，所以更宽松、也更快。
// 换用 Kryo 之后多出来的一件事是「注册」：
// 你可以提前把关心的类登记进去，让 Kryo 给它一个固定的编号并可选用自定义的序列化方式。
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import specific.WeatherRecord

// 这个接口是 Spark 提供的扩展点：实现它并把实现类的全限定名写进配置项
// 也就是 spark.kryo.registrator；Spark 启动时会用反射加载它，
// 再回调 registerClasses，把你的注册逻辑接进 Kryo。
class CustomKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    // 这里调用的 register 只是登记一个类，并没有真正改变它的序列化方式，
    // 作用是让 Kryo 用「类名 + 注册序号」来标识该类，
    // 序列化结果里不必再重复写长长的类全名，数据因此更小。
    kryo.register(classOf[WeatherRecord])
  }
}
