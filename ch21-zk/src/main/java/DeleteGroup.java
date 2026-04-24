//cc DeleteGroup 一个用于删除组及其成员的程序

import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv DeleteGroup

/**
 * 删除ZooKeeper组节点程序
 *
 * 该程序继承自ConnectionWatcher,用于删除指定的ZooKeeper组节点及其所有子节点。
 * 删除操作采用深度优先遍历,先删除所有子节点,再删除组节点本身。
 *
 * 使用说明:
 * java DeleteGroup <zookeeper地址> <组名>
 *
 * @example
 * java DeleteGroup localhost:2181 mygroup
 *
 * @see ConnectionWatcher
 */
public class DeleteGroup extends ConnectionWatcher {

    /**
     * 删除指定组及其所有成员
     *
     * 递归删除该组节点下的所有子节点,然后删除组节点本身。
     * 如果组不存在,则输出错误信息并退出程序。
     *
     * @param groupName 要删除的组名称(不含前导斜杠)
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void delete(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        try {
            // 获取组节点的所有子节点
            List<String> children = zk.getChildren(path, false);
            // 遍历删除每个子节点,-1表示不使用版本号匹配
            for (String child : children) {
                zk.delete(path + "/" + child, -1);
            }
            // 删除组节点本身
            zk.delete(path, -1);
        }
        catch (KeeperException.NoNodeException e) {
            // 组不存在,输出错误信息并退出
            System.out.printf("Group %s does not exist\n", groupName);
            System.exit(1);
        }
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     *             args[1]: 要删除的组名称
     */
    public static void main(String[] args) throws Exception {
        DeleteGroup deleteGroup = new DeleteGroup();
        deleteGroup.connect(args[0]);
        deleteGroup.delete(args[1]);
        deleteGroup.close();
    }
}
// ^^ DeleteGroup