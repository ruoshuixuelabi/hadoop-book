/**
 * 文件：DeleteGroup.java
 * 路径：src/main/java/DeleteGroup.java
 * 描述：删除 ZooKeeper 组节点程序
 * <p>
 * 功能说明：
 * 该程序继承自 ConnectionWatcher，用于删除指定的 ZooKeeper 组节点及其所有子节点。
 * 删除操作采用深度优先遍历，先删除所有子节点，再删除组节点本身。
 * <p>
 * 使用说明：
 * java DeleteGroup <zookeeper地址> <组名>
 * <p>
 * @example java DeleteGroup localhost:2181 mygroup
 */
import java.util.List;

import org.apache.zookeeper.KeeperException;

/**
 * 删除 ZooKeeper 组节点程序
 * <p>
 * 该程序继承自 ConnectionWatcher，用于删除指定的 ZooKeeper 组节点及其所有子节点。
 * 删除操作采用深度优先遍历，先删除所有子节点，再删除组节点本身。
 * <p>
 * 使用说明：
 * java DeleteGroup <zookeeper地址> <组名>
 * <p>
 * @example java DeleteGroup localhost:2181 mygroup
 */
public class DeleteGroup extends ConnectionWatcher {

    /**
     * 删除指定组及其所有成员<br>
     * <p>
     * 递归删除该组节点下的所有子节点，然后删除组节点本身。<br>
     * 如果组不存在，则输出错误信息并退出程序。
     * <p>
     * @param groupName 要删除的组名称（不含前导斜杠）
     * @throws KeeperException 如果 ZooKeeper 操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void delete(String groupName) throws KeeperException, InterruptedException {
        // 构建组节点路径
        String path = "/" + groupName;
        try {
            // 获取组节点的所有子节点
            // 第二个参数 false 表示不设置 watch
            List<String> children = zk.getChildren(path, false);
            // 遍历删除每个子节点
            // -1 表示不使用版本号匹配删除
            for (String child : children) {
                zk.delete(path + "/" + child, -1);
            }
            // 删除组节点本身
            // -1 表示不使用版本号匹配删除
            zk.delete(path, -1);
        }
        catch (KeeperException.NoNodeException e) {
            // 组不存在，输出错误信息并退出
            System.out.printf("Group %s does not exist\n", groupName);
            System.exit(1);
        }
    }

    /**
     * 主程序入口<br>
     * <p>
     * @param args args[0]: ZooKeeper 服务器地址
     *             args[1]: 要删除的组名称
     */
    public static void main(String[] args) throws Exception {
        // 创建 DeleteGroup 实例
        DeleteGroup deleteGroup = new DeleteGroup();
        // 连接 ZooKeeper 服务器
        deleteGroup.connect(args[0]);
        // 删除组
        deleteGroup.delete(args[1]);
        // 关闭连接
        deleteGroup.close();
    }
}