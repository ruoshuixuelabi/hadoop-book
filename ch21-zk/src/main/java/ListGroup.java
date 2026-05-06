//cc ListGroup 一个列出组中成员的程序

import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv ListGroup

/**
 * 列出ZooKeeper组中成员的程序
 * <p>
 * 该程序继承自ConnectionWatcher,用于获取并显示指定组下的所有成员节点名称。
 * <p>
 * 使用说明:<br/>
 * java ListGroup <zookeeper地址> <组名>
 *
 * @example
 * java ListGroup localhost:2181 mygroup
 * // 输出: member1
 * //       member2
 * //       member3
 *
 * @see ConnectionWatcher
 */
public class ListGroup extends ConnectionWatcher {

    /**
     * 列出组中的所有成员
     * <p/>
     * 获取指定组节点的所有子节点名称并打印到标准输出。<br/>
     * 如果组不存在或没有成员,则输出相应的提示信息。
     *
     * @param groupName 组名称(不含前导斜杠)
     * @throws KeeperException 如果ZooKeeper操作失败
     * @throws InterruptedException 如果操作被中断
     */
    public void list(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        try {
            // 获取组节点的所有子节点,false表示不使用watch
            List<String> children = zk.getChildren(path, false);
            if (children.isEmpty()) {
                // 组存在但没有成员
                System.out.printf("No members in group %s\n", groupName);
                System.exit(1);
            }
            // 遍历并打印每个成员名称
            for (String child : children) {
                System.out.println(child);
            }
        }
        catch (KeeperException.NoNodeException e) {
            // 组节点不存在
            System.out.printf("Group %s does not exist\n", groupName);
            System.exit(1);
        }
    }

    /**
     * 主程序入口
     *
     * @param args args[0]: ZooKeeper服务器地址
     *             args[1]: 组名称
     */
    public static void main(String[] args) throws Exception {
        ListGroup listGroup = new ListGroup();
        listGroup.connect(args[0]);
        listGroup.list(args[1]);
        listGroup.close();
    }
}
// ^^ ListGroup