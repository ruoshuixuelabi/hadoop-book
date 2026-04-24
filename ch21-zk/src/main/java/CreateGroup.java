//cc CreateGroup 一个在ZooKeeper中创建表示组的znode的程序

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

// vv CreateGroup

/**
 * 创建ZooKeeper组节点程序
 *
 * 该程序实现Watcher接口,用于在ZooKeeper中创建一个持久化的组节点。
 * 演示了ZooKeeper基本的节点创建操作。
 *
 * 使用说明:
 * java CreateGroup <zookeeper地址> <组名>
 *
 * @example
 * java CreateGroup localhost:2181 mygroup
 * // 将在ZooKeeper中创建节点: /mygroup
 */
public class CreateGroup implements Watcher {

    /** 会话超时时间,单位毫秒 */
    private static final int SESSION_TIMEOUT = 5000;

    /** ZooKeeper客户端实例 */
    private ZooKeeper zk;

    /** 连接信号量,用于阻塞等待连接建立 */
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接到ZooKeeper服务器
     *
     * @param hosts ZooKeeper服务器地址列表
     * @throws IOException 如果发生I/O错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    /**
     * 处理ZooKeeper事件回调
     *
     * @param event ZooKeeper事件
     */
    @Override
    public void process(WatchedEvent event) { // Watcher interface
        // 检查是否为连接建立事件
        if (event.getState() == KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    /**
     * 创建组节点
     *
     * 在ZooKeeper中创建一个持久化节点,节点数据为空。
     *
     * @param groupName 要创建的组名称(不含前导斜杠)
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void create(String groupName) throws KeeperException,
            InterruptedException {
        String path = "/" + groupName;
        // 创建持久节点,无 ACL 控制,无数据
        String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        System.out.println("Created " + createdPath);
    }

    /**
     * 关闭ZooKeeper连接
     *
     * @throws InterruptedException 如果关闭过程中被中断
     */
    public void close() throws InterruptedException {
        zk.close();
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     *             args[1]: 要创建的组名称
     */
    public static void main(String[] args) throws Exception {
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(args[0]);
        createGroup.create(args[1]);
        createGroup.close();
    }
}
// ^^ CreateGroup