/**
 * 文件：CreateGroup.java
 * 路径：src/main/java/CreateGroup.java
 * 描述：在 ZooKeeper 中创建组成员节点
 * <p>
 * 功能说明：
 * 该程序实现 Watcher 接口，用于在 ZooKeeper 中创建一个持久化的组节点。
 * 演示了 ZooKeeper 基本的节点创建操作。
 * <p>
 * 使用说明：
 * java CreateGroup <zookeeper地址> <组名>
 * <p>
 * @example java CreateGroup localhost:2181 mygroup
 *          // 将在 ZooKeeper 中创建节点: /mygroup
 */
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * 创建 ZooKeeper 组节点程序
 * <p>
 * 该程序实现 Watcher 接口，用于在 ZooKeeper 中创建一个持久化的组节点。
 * 演示了 ZooKeeper 基本的节点创建操作。
 * <p>
 * 使用说明：
 * java CreateGroup <zookeeper地址> <组名>
 * <p>
 * @example java CreateGroup localhost:2181 mygroup
 *          // 将在 ZooKeeper 中创建节点: /mygroup
 */
public class CreateGroup implements Watcher {

    /**
     * 会话超时时间，单位毫秒<br>
     * 客户端与服务器通信的超时时间
     */
    private static final int SESSION_TIMEOUT = 5000;

    /**
     * ZooKeeper 客户端实例<br>
     * 用于与 ZooKeeper 服务器通信，执行节点操作
     */
    private ZooKeeper zk;

    /**
     * 连接信号量，用于阻塞等待连接建立<br>
     * CountDownLatch 初始化为 1，当收到连接成功事件时倒计数归零
     */
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接到 ZooKeeper 服务器<br>
     * <p>
     * 创建 ZooKeeper 客户端实例并等待连接建立。
     * <p>
     * @param hosts ZooKeeper 服务器地址列表
     * @throws IOException 如果发生 I/O 错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        // 创建 ZooKeeper 客户端实例，注册当前 watcher
        // 构造参数：主机地址列表、会话超时时间、watcher 回调
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        // 阻塞等待连接建立完成
        connectedSignal.await();
    }

    /**
     * 处理 ZooKeeper 事件回调<br>
     * <p>
     * 当收到 SyncConnected 状态事件时，释放信号量表示连接就绪。
     * <p>
     * @param event ZooKeeper 事件对象
     */
    @Override
    public void process(WatchedEvent event) {
        // 检查是否为连接建立事件
        // KeeperState.SyncConnected 表示客户端与服务器连接成功
        if (event.getState() == KeeperState.SyncConnected) {
            // 连接已建立，倒计数归零
            connectedSignal.countDown();
        }
    }

    /**
     * 创建组节点<br>
     * <p>
     * 在 ZooKeeper 中创建一个持久化节点，节点数据为空。
     * <p>
     * @param groupName 要创建的组名称（不含前导斜杠）
     * @throws KeeperException 如果 ZooKeeper 操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void create(String groupName) throws KeeperException,
            InterruptedException {
        // 构建节点路径："/" + 组名
        String path = "/" + groupName;
        // 创建持久节点，无 ACL 控制，无数据
        // Ids.OPEN_ACL_UNSAFE 表示完全开放的 ACL，任何客户端都可以访问
        // CreateMode.PERSISTENT 表示持久节点，不会因客户端断开而删除
        String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        // 打印创建的节点路径
        System.out.println("Created " + createdPath);
    }

    /**
     * 关闭 ZooKeeper 连接<br>
     * <p>
     * 关闭客户端连接，释放资源。
     * <p>
     * @throws InterruptedException 如果关闭过程中被中断
     */
    public void close() throws InterruptedException {
        // 关闭 ZooKeeper 客户端连接
        zk.close();
    }

    /**
     * 主程序入口<br>
     * <p>
     * @param args args[0]: ZooKeeper 服务器地址
     *             args[1]: 要创建的组名称
     */
    public static void main(String[] args) throws Exception {
        // 创建 CreateGroup 实例
        CreateGroup createGroup = new CreateGroup();
        // 连接 ZooKeeper 服务器
        createGroup.connect(args[0]);
        // 创建组节点
        createGroup.create(args[1]);
        // 关闭连接
        createGroup.close();
    }
}