/**
 * 文件：CustomKryoRegistrator.scala<br>
 * 路径：src/test/scala/CustomKryoRegistrator.scala<br>
 * 描述：自定义Kryo序列化注册器<br>
 *<br>
 * 功能说明：<br>
 * Kryo是Spark默认的高效序列化库，相比Java序列化速度更快、体积更小。<br>
 * 该注册器用于将自定义的WeatherRecord类注册到Kryo，以便在RDD序列化时使用。<br>
 * 通过预注册类，Kryo可以生成更紧凑的序列化格式，减少网络传输和内存开销。<br>
 *<br>
 * 使用方式：<br>
 * 在SparkConf中设置spark.serializer为KryoSerializer，<br>
 * 并设置spark.kryo.registrator指向该类。<br>
 */
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import specific.WeatherRecord

/**
 * 自定义Kryo序列化注册器类<br>
 * 继承KryoRegistrator特质，实现registerClasses方法<br>
 * 用于注册需要使用Kryo序列化的自定义类型<br>
 */
class CustomKryoRegistrator extends KryoRegistrator {
  /**
   * 注册需要序列化的类<br>
   *<br>
   * @param kryo Kryo实例，用于配置序列化行为<br>
   */
  override def registerClasses(kryo: Kryo) {
    // 将WeatherRecord类注册到Kryo
    // 注册后Kryo在序列化该类时会使用其注册的ID，生成更紧凑的二进制格式
    kryo.register(classOf[WeatherRecord])
  }
}
