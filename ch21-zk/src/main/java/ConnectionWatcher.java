/**
 * 文件：ConnectionWatcher.java
 * 路径：src/main/java/ConnectionWatcher.java
 * 描述：ZooKeeper 连接监视器类
 * <p>
 * 功能说明：
 * 该类实现了 Watcher 接口，用于处理 ZooKeeper 连接事件。
 * 主要功能是等待 ZooKeeper 客户端与服务器之间的连接成功建立。
 * <p>
 * 使用方式：
 * 1. 创建 ConnectionWatcher 实例
 * 2. 调用 connect(hosts) 方法连接到 ZooKeeper 服务器
 * 3. 连接建立后，connectedSignal 倒计数器会归零，表示连接就绪
 * 4. 操作完成后调用 close() 方法关闭连接
 * <p>
 * 运行命令：
 * java ConnectionWatcher <zookeeper地址>
 */
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

/**
 * ZooKeeper 连接监视器类
 * <p>
 * 该类实现了 Watcher 接口，用于处理 ZooKeeper 连接事件。
 * 主要功能是等待 ZooKeeper 客户端与服务器之间的连接成功建立。
 * <p>
 * 使用说明：
 * 1. 创建 ConnectionWatcher 实例
 * 2. 调用 connect(hosts) 方法连接到 ZooKeeper 服务器
 * 3. 连接建立后，connectedSignal 倒计数器会归零，表示连接就绪
 * 4. 操作完成后调用 close() 方法关闭连接
 * <p>
 * @example ConnectionWatcher watcher = new ConnectionWatcher();
 *          watcher.connect("localhost:2181");
 *          // 执行ZooKeeper操作...
 *          watcher.close();
 */
public class ConnectionWatcher implements Watcher {

    /**
     * 会话超时时间，单位毫秒<br>
     * 客户端与服务器通信的超时时间，如果超过此时间没有通信则会话失效
     */
    private static final int SESSION_TIMEOUT = 5000;

    /**
     * ZooKeeper 客户端实例<br>
     * 用于与 ZooKeeper 服务器通信，执行节点操作
     */
    protected ZooKeeper zk;

    /**
     * 连接信号量，用于阻塞等待连接建立<br>
     * CountDownLatch 初始化为 1，当收到连接成功事件时倒计数归零<br>
     * 用于在 connect() 方法中阻塞等待，直到连接建立完成
     */
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 连接到 ZooKeeper 服务器<br>
     * <p>
     * 创建 ZooKeeper 客户端实例并注册当前 watcher，然后阻塞等待连接建立完成。<br>
     * 当 connectedSignal 倒计数归零时，表示连接已就绪。
     * <p>
     * @param hosts ZooKeeper 服务器地址列表，格式如 "host1:port1,host2:port2"
     * @throws IOException 如果发生 I/O 错误
     * @throws InterruptedException 如果等待过程中被中断
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        // 创建 ZooKeeper 客户端实例，注册当前 watcher
        // ZooKeeper 构造参数：主机地址列表、会话超时时间、watcher 回调
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        // 阻塞等待连接建立完成
        // 只有当 process() 收到 SyncConnected 事件时，connectedSignal 才会倒计数归零
        connectedSignal.await();
    }

    /**
     * 处理 ZooKeeper 事件回调<br>
     * <p>
     * 当收到 SyncConnected 状态事件时，表示连接已建立，释放信号量。<br>
     * 其他事件类型暂不处理。
     * <p>
     * @param event ZooKeeper 事件对象，包含事件类型、状态等信息
     */
    @Override
    public void process(WatchedEvent event) {
        // 检查是否为连接建立事件
        // KeeperState.SyncConnected 表示客户端与服务器连接成功
        if (event.getState() == KeeperState.SyncConnected) {
            // 连接已建立，倒计数归零，解除 connect() 方法的阻塞状态
            connectedSignal.countDown();
        }
    }

    /**
     * 关闭 ZooKeeper 连接<br>
     * <p>
     * 关闭客户端连接，释放资源。<br>
     * 调用此方法后，客户端将断开与服务器的连接。
     * <p>
     * @throws InterruptedException 如果关闭过程中被中断
     */
    public void close() throws InterruptedException {
        // 关闭 ZooKeeper 客户端连接
        // 此方法会阻塞直到所有资源释放
        zk.close();
    }
}