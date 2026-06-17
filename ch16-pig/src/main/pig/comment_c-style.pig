/*
 * comment_c-style.pig: 演示Pig Latin支持的C风格多行注释
 * 此脚本展示了如何使用/* */注释样式
 */

/*
 * Description of my program spanning
 * multiple lines.
 */

// 步骤1: 加载表A的数据
A = LOAD 'input/pig/join/A';
// 步骤2: 加载表B的数据
B = LOAD 'input/pig/join/B';
// 步骤3: 基于A的第0列和B的第1列进行连接
C = JOIN A BY $0, /* ignored */ B BY $1;
// 步骤4: 输出连接结果
DUMP C;
