# 本脚本演示 ZooKeeper 里的群组概念：先建一个组，再让三个成员加入，
# 接着杀掉其中一个成员，看群组如何自动把它去掉，最后把整个组删掉。
# 下面以冒号开头的几行是书稿抽取用的标记，不是 shell 语法。
: == group_create
: == group_list_empty
: == group_join
: == group_list_after_join
: == group_kill_goat
: == group_list_after_kill
: == group_delete
: vv group_create
# 建组：CreateGroup 在 ZooKeeper 里创建一个名为 zoo 的持久节点。
# 之所以用持久节点而不是临时节点，是因为组本身要活得比它的成员久，
# 成员来来去去不该把组也一并带走。
java CreateGroup localhost zoo
: ^^ group_create
: vv group_list_empty
# 列成员：刚建好的组是空的，ListGroup 会打印一行提示并用非零状态退出，
# 这样脚本里就能一眼看出「组还在，但一个成员都没有」。
java ListGroup localhost zoo
: ^^ group_list_empty
: vv group_join
# 加入：三个成员进程都放到后台跑（行尾的 & 让 shell 不等它们结束），
# 这个成员会在组节点下面建一个以成员名命名的临时节点，然后一直阻塞着。
# 用临时节点的好处是成员进程一死，这个成员就自动从组里消失了。
java JoinGroup localhost zoo duck &
# 紧跟着的赋值里，$! 就是刚才那个后台进程的进程号，留着等会儿要靠它来结束成员。
duck_pid=$!
java JoinGroup localhost zoo cow &
cow_pid=$!
java JoinGroup localhost zoo goat &
goat_pid=$!
: ^^ group_join
# 等一小会儿再往下走：三个成员是并行启动的，
# 要给它们时间把临时节点注册上去，不然紧接着去列成员可能看到空列表。
sleep 5
: vv group_list_after_join
# 三个成员都在了，这次列出来应该能看到 duck、cow 和 goat 三行。
java ListGroup localhost zoo
: ^^ group_list_after_join
: vv group_kill_goat
# 杀掉 goat 的进程：它的临时节点会随会话结束而自动消失，
# 这就是「退出群组」——不需要额外写退群代码。
kill $goat_pid
: ^^ group_kill_goat
# 客户端程序被强杀时，ZooKeeper 要等会话超时（示例里是 5 秒）才会清理临时节点，
# 所以这里连等两个 5 秒，确保 goat 的节点真的已经消失，再去列成员才不会看到残留。
sleep 5
sleep 5 # be sure goat process has died
: vv group_list_after_kill
# 这时候列表里应该只剩 duck 和 cow，goat 已经自己退出了。
java ListGroup localhost zoo
: ^^ group_list_after_kill
# 剩下两个成员也一并结束掉，后面的删组就不必再操心遗留的临时节点。
kill $duck_pid
kill $cow_pid
: vv group_delete
# 删组：DeleteGroup 会先删掉组里的子节点，再删组本身——
# 这是因为 ZooKeeper 不允许直接删一个还带着子节点的节点。
java DeleteGroup localhost zoo
# 最后再列一次：组已经没了，ListGroup 会报告组不存在并以非零状态退出。
java ListGroup localhost zoo
: ^^ group_delete