/**
 * 文件：ActiveKeyValueStore.java
 * 路径：src/main/java/ActiveKeyValueStore.java
 * 描述：活跃键值存储类
 * <p>
 * 功能说明：
 * 封装了对 ZooKeeper 节点的读写操作，提供简洁的 API。
 * 使用 ConnectionWatcher 作为基类，继承 ZooKeeper 连接管理功能。
 * <p>
 * 主要功能：
 * - write(): 写入数据到指定路径（支持创建或更新）
 * - read(): 读取指定路径的数据（支持设置 watcher）
 * <p>
 * @see ConnectionWatcher
 * @see ResilientActiveKeyValueStore
 */
import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 活跃键值存储类
 * <p>
 * 封装了对 ZooKeeper 节点的读写操作，提供简洁的 API。
 * 使用 ConnectionWatcher 作为基类，继承 ZooKeeper 连接管理功能。
 * <p>
 * 主要功能：
 * - write(): 写入数据到指定路径（支持创建或更新）
 * - read(): 读取指定路径的数据（支持设置 watcher）
 */
public class ActiveKeyValueStore extends ConnectionWatcher {

    /**
     * UTF-8 字符集编码<br>
     * 用于将字符串与字节数组之间进行转换
     */
    private static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * 写入数据到指定路径<br>
     * <p>
     * 如果节点不存在则创建，存在则更新。-1 表示使用任意版本号进行更新。
     * <p>
     * @param path 节点路径
     * @param value 要写入的数据值
     * @throws InterruptedException 如果操作被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败
     */
    public void write(String path, String value) throws InterruptedException,
            KeeperException {
        // 检查节点是否存在
        // 第二个参数 false 表示不设置 watch
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            // 节点不存在，创建新节点
            // CreateMode.PERSISTENT 表示持久节点
            zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } else {
            // 节点存在，-1 表示不关心版本号，直接更新
            zk.setData(path, value.getBytes(CHARSET), -1);
        }
    }

    /**
     * 读取指定路径的数据<br>
     * <p>
     * 设置 watcher 用于监视数据变化。
     * <p>
     * @param path 节点路径
     * @param watcher 用于监视数据变化的 watcher，传入 null 表示不监视
     * @return 节点的数据内容
     * @throws InterruptedException 如果操作被中断
     * @throws KeeperException 如果 ZooKeeper 操作失败
     */
    public String read(String path, Watcher watcher) throws InterruptedException,
            KeeperException {
        // 读取数据并注册 watcher 用于监视变化
        // 第三个参数 null 表示不返回 Stat 信息
        byte[] data = zk.getData(path, watcher, null/*stat*/);
        return new String(data, CHARSET);
    }
}