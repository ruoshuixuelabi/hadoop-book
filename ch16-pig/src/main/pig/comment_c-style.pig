-- 这个文件演示 Pig 的块注释写法：注释可以跨行，也可以写在语句中间。
-- 下面那段英文的说明就是用一对块注释符号括起来的。
/* 
 * Description of my program spanning
 * multiple lines.
 */
A = LOAD 'input/pig/join/A';
B = LOAD 'input/pig/join/B';
C = JOIN A BY $0, /* ignored */ B BY $1;
DUMP C;