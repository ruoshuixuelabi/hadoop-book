# hadoop-book 中文教学注释化设计文档

日期：2026-10-26
状态：**已实现并提交**（详见第九节「实施结果」）；待用户复核批准

## 一、意图与背景

`hadoop-book` 是《Hadoop: The Definitive Guide, Fourth Edition》的示例代码仓库，
当前处于上游 `tomwhite/hadoop-book` 的 `upstream/update`（d72fad4, 2017-10-13）纯净状态。

用户正在系统学习 Hadoop，需要读懂每一章的示例代码。当前仓库里
**234 个 Java 文件、约 17,000 行代码，注释几乎为零**（全仓库仅 18 个文件含 Javadoc 块）。

本次工作的目标：为全部 Java 源码补充中文教学注释，使读者能脱离书籍正文独立读懂代码。

**为什么是"只加注释"**：上一轮 AI 曾经尝试过大规模改写这个仓库，但同时做了
注释添加、代码格式化和依赖升级三件事，结果破坏性变更与注释混在一起无法区分，
最终被整体回滚。本次严格限定：**只允许新增/修改注释，源码逻辑一行不动**。

## 二、目标与非目标

### 目标
- 全部 Java 源码（`src/main/java` + `src/test/java`）覆盖中文教学注释
- 类级 Javadoc、方法级 Javadoc、方法内部行内注释三者格式严格区分
- 注释解释"为什么"和"业务背景"，而非翻译代码
- 保留上游已有的英文注释，原样不动
- 每个模块一个 commit，落在 master 上
- 每步都有机器校验证明源码逻辑零改动

### 非目标（明确排除）
- 不修改任何代码逻辑、签名、导入顺序、格式
- 不升级/降级任何依赖版本
- 不删除或改写上游已有英文注释
- 不改 pom.xml、不改构建配置
- 不重构、不"顺手优化"任何东西
- 不给 Lombok 生成的 getter/setter 加注释
- 不添加测试代码、文档站点、README 等新文件

## 三、约束

### 3.1 源码不变性（硬约束）
用 `E:/tmp/verify_comments_only.py` 校验：把每个 Java 文件按 Java 词法规则拆成
**代码 token 流**（剥掉注释与空白，字符串/字符字面量保留原样），
与基线快照（`E:/tmp/baseline-java`）的 token 流逐 token 对比，必须完全相同。

该脚本已做六向自测，全部通过：

| 场景 | 期望 | 实测 |
|---|---|---|
| 当前树（未改动） | 放行 | EXIT=0 |
| 改类名 `MaxTemperatureMapper`→`...MapperX` | 检出 | EXIT=1，定位到 token 209 |
| 改字符串字面量 `glob("/*")`→`glob("/**")` | 检出 | EXIT=1，定位到 token 849 |
| 只加一行普通注释 | 放行 | EXIT=0 |
| 新增 `// cc 统计最高气温` 注释行 | 检出 | EXIT=1（PHRAGMITE RISK） |
| 新增 `// == 这是说明` 注释行 | 检出 | EXIT=1（PHRAGMITE RISK） |

**为什么用 token 流而不是简单的行过滤**：字符串字面量里可能含 `//` 或 `/*`。
例如 `ch05-io/src/test/java/TextTest.java:55`：
```java
t.set(/*[*/new Text("pig")/*]*/);
```
朴素的「剥掉以 `//` 或 `/*` 开头的行」会把它当成注释起点，校验直接失真。

注意：**不能用字节码比对做判据**。加注释必然改变 `LineNumberTable` 行号，
已实测导致 `.class` 文件二进制不同，但代码逻辑并未改变。行号变化是预期行为，不是缺陷。

### 3.2 书稿清单机制（硬约束，探查后新增）
`snippet/bin/` 下的工具链用于把代码抽成书里的程序清单：

- `phragmite_db.pl` 按 `// vv <名字>` … `// ^^ <名字>` 标记**界定清单抽取范围**，
  范围内的内容会被抽成 `snippet/actual/chNN/<名字>.xml`
- `check_manuscript.py` 再把抽出的片段与书稿 XML 比对，确认书里印的代码与仓库一致
- `generate_listings.sh` 会对 ch02/ch03/ch05/ch06/ch08/ch09/ch12/ch21/common
  共 84 个带标记的文件执行抽取

**两条实际约束**：

1. **中文注释会进入书稿片段**。带 `// vv`/`// ^^` 标记的 84 个文件里，
   我们加的中文注释也会被抽进 XML。这会让 `check_manuscript.py` 报
   "Snippet not found"——因为书稿里印的是英文原版。
   但该工具链需要 `~/book-workspace/htdg-git`（O'Reilly 书稿工作区），
   本机不存在，**当前无法运行**，也不在 Maven 构建流程里
   （`book/pom.xml` 无 exec 插件引用这些脚本）。
   结论：不影响构建与测试，属于"若将来要重新出版书稿"才需处理的事项，
   在设计文档中记录即可，不阻塞本次工作。

2. **注释行不能以书稿标记开头**。`phragmite_db.pl` 的识别正则是：
   ```
   ^\s*//\s*(vv+|\^\^+|xx+|cc+(?:\[.*\])?|hh+|!!+|\?\?+(?:\[.*\])?|==+|--+)\s+(\S*)
   ```
   若新增注释写成 `// cc 统计最高气温` 或 `// == 说明`，
   会被误判为书稿指令（caption/options），破坏清单抽取。
   **规避办法**：中文注释一律以中文字符开头，不用 `// cc`、`// ==`、
   `// vv`、`// !!` 这类前缀。校验脚本已内置这条检测（见 3.1 表格末两行）。
   同理，注释里避免出现 `/*[*/`、`/*]*/`、`/*...*/`、`/*public*/`、`/*private*/`
   这五个会被 perl 脚本做文本替换的标记。

3. **Javadoc 会被折叠成书稿的加粗标记**。perl 脚本中有这条替换规则：
   ```perl
   $line =~ s!/\*\*\s*(\S+(?:\s+\S+)*)\s*\*\*/!/\* \`{$1}\` \*/!g;
   ```
   单行 Javadoc 会被转成书稿里的加粗文本。这同样只影响将来的书稿抽取，
   不影响编译与测试。

4. **中文注释一律放在 `// vv` / `// ^^` 标记之外吗？**——不需要刻意规避。
   标记内外的注释都会被抽取，区别仅在于书稿里是否显示。
   本次统一放在标记内（紧贴代码），保证阅读源码时注释与代码相邻。

### 3.3 已有英文注释的处理
上游 18 个文件里已有 Javadoc 块或行内英文注释（如 `ch22-case-studies` 的
`TrackStatisticsProgram.java` 有 16 处 Javadoc + 16 处行内注释）。
**一律原样保留**，在其上方或旁边补充中文说明，不改写、不删除。

### 3.4 构建环境
- 必须用 JDK 8：`export JAVA_HOME="/e/Program Files/Java/jdk-1.8"`
- Maven 本地仓库在 `e:\repository`（非 `~/.m2/repository`）
- 全量 `mvn clean package -DskipTests` 当前 21 模块全绿（已验证）
- 已有两个环境性失败（`ch06` MiniYARNCluster 需要管理员符号链接权限、
  `ch05-io` FileDecompressorTest 的 Windows 路径 URI 上游 bug），非本次引入
- `mvn verify` 会触发 failsafe 的 `snippet/ExamplesIT`，但该测试需要
  `HADOOP_HOME` 环境变量和 `snippet/src/main/examples` 下的示例目录，
  本机两者皆无（该目录为空），故集成测试当前无法运行——既有环境状态，非本次引入

### 3.5 注释规范来源
沿用用户本地的 `java-comment` skill 规范（`~/.claude/skills/java-comment/SKILL.md`）：
- 中文为主，Javadoc 描述与标签全中文
- 禁用 `<br>`，用 `<p>` 分段
- 行内注释放**代码上一行**，不放行尾、不放下方
- 已有英文注释严格保留
- 不逐行翻译代码
- Javadoc 是契约（做什么/为什么/约束），实现细节放行内注释
- 公共 API 必有 Javadoc，private 方法仅在命名不足以表达意图时加

### 3.6 用户已确认的决定
| 项 | 决定 |
|---|---|
| 注释语言 | 中文为主，保留原有英文 |
| 覆盖范围 | `src/main/java` 全部 + `src/test/java` 全部（含 `oldapi/`） |
| 注释密度 | 教学型（类 Javadoc + public 方法完整 Javadoc + 关键步骤行内注释） |
| 落地方式 | 每个模块一个 commit，直接提交到 master |
| 推进顺序 | 按书章节顺序 ch02 → ch03 → ch05 → … |

### 3.7 实施中确定下来的细则
以下条目在实施过程中被具体化并全程遵守：

| 项 | 细则 |
|---|---|
| 覆盖范围 | `src/main/java` + `src/test/java` 全部 Java 文件，**含 `common/src/main/java/oldapi/` 下的旧 API 副本**；不涉及 `.scala`/`.pig`/`.hive`/`.avsc`/`.py`/`.r` 等非 Java 文件 |
| 三种形态 | 类/接口/枚举 → 类级 Javadoc；public/protected 方法 → 完整 Javadoc（含 `@param`/`@return`/`@throws`）；方法体内部 → `//` 行内注释，说明「为什么」而不是「是什么」 |
| Javadoc 位置 | 必须放在**所有注解之前**（`@Test`、`@Override`、`@Before`、`@SuppressWarnings` 等），否则 Javadoc 不与该声明绑定 |
| Javadoc 缩进 | 设声明行缩进为 d，则 `/**` 缩进 d，`*` 行缩进 d+1，`*/` 缩进 d+1（闭括号与正文星号对齐，不与 `/**` 对齐） |
| 禁用标签 | 不写 `@author`、`@since`、`@version`、`@date`（上游风格里没有，徒增噪音） |
| 合并既有英文 Javadoc | 中文段落写在前，原英文作为第二段保留原样；中文缩进与英文正文缩进保持一致 |
| 行内注释位置 | 放在被注释代码的**上一行**，缩进与下一行**代码**对齐（不是与上一行对齐，也不是与注释自身对齐） |
| 书稿标记 | 任何新增注释行都不得以 `// --`、`// ==`、`// vv`、`// ^^`、`// xx`、`// hh`、`// !!`、`// ??` 等 phragmite 标记开头；中文注释一律以中文字符开头 |
| 只增不改 | 只允许插入注释；注释以外的代码 token 流必须与基线**逐 token 完全一致** |
| 换行与编码 | UTF-8 无 BOM，LF 换行（`core.autocrlf=true`，工作区文件保持 LF） |

## 四、注释的三种形态（格式必须区分）

这是本次最核心的要求。三者格式不可混用：

### 4.1 类/接口/枚举级 Javadoc
位置：紧贴类型声明**上方**。
```java
/**
 * 统计每个气象站的最高气温
 *
 * <p>Mapper 阶段：逐行解析 NCDC 原始记录，以「年份+站号」为 key、
 * 气温为 value 输出。由于需要比较整年记录，故 key 中必须包含年份
 * <p>Reducer 阶段：同一站同一年的所有记录汇聚到一块，取最大值
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
```

### 4.2 方法级 Javadoc
位置：紧贴方法声明**上方**，独立于类级。
必须包含 `@param`（每个参数一行）、`@return`（有返回值时）、`@throws`（有受检异常时）。
```java
/**
 * 从一行 NCDC 原始记录中提取温度值
 *
 * <p>记录格式形如 "0101 01010199999 021603106 265 15"，温度固定在第 4 个字段。
 * 质量位非 '1' 表示读数可疑，此处直接返回缺省值让下游忽略该条记录
 *
 * @param row 原始记录行，以空白分隔
 * @return 摄氏温度，缺省为 0 表示该记录无效
 */
public int parseTemperature(String row) {
```

### 4.3 方法内部行内注释
位置：代码**上一行**，`//` 开头，单行，不带标签。
解释"为什么这么做"，不重复代码字面意思。
```java
// Combiner 必须在 Map 端做局部归约，才能显著减少 Shuffle 的网络传输量；
// 它要求聚合函数满足结合律和交换律，取最大值满足这两个条件
public class MaxTemperatureWithCombiner {
```

字段注释用单行 `/** ... */` 形式，置于字段声明上方。

## 五、工作流程

### 5.1 每个模块的六步
1. **读取**：Read 整个文件的完整内容（不局部读）
2. **识别**：列出类/方法/字段，标注已有 Javadoc、已有英文注释
3. **加注释**：用「注释插入规格文件 + 脚本化落盘」的方式批量插入，
   避免手改大文件时误触代码行
4. **校验**：跑 `python E:/tmp/verify_comments_only.py .` → 必须 EXIT=0；
   再跑 `python E:/tmp/check_indent.py .` → 行内注释缩进必须 0 mismatch
5. **构建**：`mvn -o -pl <module> -am test-compile`（JDK 8）→ 必须 BUILD SUCCESS
6. **提交**：单个 commit，message 说明本章学到了什么

### 5.1.1 注释插入工具链（实施中定型）
规格文件（`E:/tmp/spec_*.txt`）格式：

```
# file: <仓库相对路径>
@@ <锚点行>          ← 定位插入位置（整行去空白后精确匹配）
## <注释内容>        ← 要插入的行，缩进由「##」后的空格数决定
```

- `##` 组挂在**上一个** `@@` 块上，插在该块**之前**
- `@@ >锚点` 表示插到锚点行**之后**（用于 catch/else 分支内部）
- `@@ %N 锚点` 表示取第 N 个（0 基）匹配项，用于同形重复行
- 连续多个 `@@` 会合并成一个多行锚点；**空行会打断锚点链**
- 落盘前脚本会做两阶段校验：先全量验证所有文件**零问题**才写盘

配套脚本：`add_comments.py`（落盘）、`check_indent.py`（缩进检查）、
`verify_comments_only.py`（token 流 + 风险检查）、`auto_occ.py`（歧义锚点编号）、
`fix_spec_indent.py`（缩进自动纠正）、`fix_noncomment.py`、`strip_occ.py`。

### 5.2 模块顺序与规模
按书章节顺序，共 16 个含 Java 代码的模块：

| 序 | 模块 | Java 文件 | 行数 | 章节主题 |
|---|---|---|---|---|
| 1 | ch02-mr-intro | 9 | 333 | MapReduce 入门 |
| 2 | ch03-hdfs | 12 | 565 | HDFS API |
| 3 | ch05-io | 36 | 1682 | IO 格式（Writable/SequenceFile/MapFile） |
| 4 | ch06-mr-dev | 21 | 896 | MR 开发（v1/v2 API、Combiner、Partitioner） |
| 5 | ch08-mr-types | 22 | 1188 | MR 类型与输入输出格式 |
| 6 | ch09-mr-features | 32 | 1654 | MR 特性（计数器/排序/连接/Combiner） |
| 7 | ch12-avro | 11 | 1801 | Avro 序列化 |
| 8 | ch13-parquet | 7 | 596 | Parquet 列式存储 |
| 9 | ch15-sqoop | 3 | 773 | Sqoop |
| 10 | ch16-pig | 6 | 287 | Pig |
| 11 | ch17-hive | 3 | 126 | Hive |
| 12 | ch18-crunch | 33 | 2571 | Crunch Pipeline API |
| 13 | ch19-spark | 4 | 507 | Spark |
| 14 | ch20-hbase | 13 | 843 | HBase |
| 15 | ch21-zk | 10 | 397 | ZooKeeper |
| 16 | ch22-case-studies | 2 | 738 | 案例研究（航班延误统计） |

另有两个含 Java 的模块：`common`（13 文件 745 行，共享的 NCDC 记录解析器等），
被 ch08/ch09/ch20 依赖；`snippet`（1 个集成测试 `ExamplesIT.java`）。

无 Java 代码的目录（ch04-yarn、ch10-setup、ch14-flume、hadoop-examples、hadoop-meta）
不在本次范围内——它们只有 XML 配置或脚本。

**`common` 放在最后处理**：它是其他模块的依赖，被注释后不改变行为，先做或后做都可以；
放在 ch02-ch22 之后可以让主线（按章节阅读）不被打断。

### 5.3 大模块的处理
ch18-crunch（2571 行）、ch09-mr-features（1654 行）、ch05-io（1682 行）
超过单次上下文舒适区，按包/类簇分批处理，但**仍合并为该模块的单个 commit**。

### 5.4 注释书写红线（每次落盘前自查）
1. 中文行内注释**以中文字符开头**，绝不以 `// cc` `// ==` `// vv` `// ^^`
   `// xx` `// hh` `// !!` `// ??` `// --` 开头（会被 phragmite_db.pl 误判）
2. 注释正文避开这五个会被 perl 替换的标记：
   `/*[*/` `/*]*/` `/*...*/` `/*public*/` `/*private*/`
3. Javadoc 中禁用 `<br>`，用 `<p>` 分段
4. Javadoc 里不写 `TODO`，TODO 用 `// TODO:` 行内注释
5. 注释一律放代码**上一行**，不放行尾
6. 中文用 UTF-8 写入，不引入 BOM、不改换行符（保持 LF）
7. Javadoc 必须在注解**之前**，闭括号 `*/` 与正文 `*` 对齐
8. 不写 `@author`/`@since`/`@version`/`@date`

## 六、验收标准

每完成一个模块，必须同时满足：
1. `python E:/tmp/verify_comments_only.py .` → EXIT=0（代码 token 流零改动 + 无书稿标记冲突）
2. `python E:/tmp/check_indent.py .` → `0 mismatches`（行内注释缩进与代码对齐）
3. `mvn -o -pl <module> -am test-compile` → BUILD SUCCESS（注释没破坏语法）
4. `git diff --stat` 显示的改动全部落在注释行上
5. 公共类有类级 Javadoc；public/protected 方法有完整 Javadoc（含标签）
6. 关键步骤有上一行行内注释，解释"为什么"
7. 上游已有英文注释一字未动
8. 该模块恰好一个 commit

最终全量校验：`mvn -o test-compile` 全反应堆 21 模块 BUILD SUCCESS
+ verify 脚本 EXIT=0 + check_indent 0 mismatch。

## 七、留给实现者的判断空间

以下由实现者自行决定，不需回头问：
- 具体某处注释的措辞
- 行内注释的插入位置（只要在代码上一行）
- private 方法是否加注释（按命名是否自明判断）
- 章节主题的引述方式
- 大量重复类（如 `oldapi/` 与 v2 API 的对应实现）是否用"对应 v2 版本"作交叉引用

## 八、风险与对策

| 风险 | 对策 |
|---|---|
| 注释写错导致语义误导 | 每批写完后回读比对，注释必须与代码逻辑一致 |
| 中文注释被误判为书稿标记 | 注释以中文开头；校验脚本内置 PHRAGMITE RISK 检测 |
| 中文注释进入书稿清单片段 | 已记录于 3.2；不影响构建，若将来重新出版需重新抽取清单 |
| 大模块上下文溢出 | 按类簇分批，最终合并单 commit |
| 误改代码 | verify 脚本逐文件比对，EXIT≠0 即回滚该批 |
| 注释破坏编译（如 Javadoc 中出现 `*/`） | 每批后立即 compile |
| 用户中途改变主意 | 每个模块独立 commit，可单独 revert |

## 九、实施结果

### 9.1 已完成的提交
18 个 commit，全部落在 `master`，按书章节顺序：

| # | Commit | 模块 |
|---|---|---|
| 1 | `a5e823a` | ch02-mr-intro |
| 2 | `9694e09` | ch03-hdfs |
| 3 | `936eda8` | ch05-io |
| 4 | `459c468` | ch06-mr-dev |
| 5 | `941b329` | ch08-mr-types |
| 6 | `12b9f2d` | ch09-mr-features |
| 7 | `3eb0758` | ch12-avro |
| 8 | `9e500a0` | ch13-parquet |
| 9 | `ddc6cdb` | ch15-sqoop |
| 10 | `b96ade7` | ch16-pig |
| 11 | `5bd7f9c` | ch17-hive |
| 12 | `1bcd124` | ch18-crunch（33 文件） |
| 13 | `33736b1` | ch19-spark |
| 14 | `29ab270` | ch20-hbase |
| 15 | `aee03f6` | ch21-zk |
| 16 | `1f092b7` | ch22-case-studies |
| 17 | `c3591a4` | common（含 oldapi/） |
| 18 | `9c7e883` | snippet |

### 9.2 最终校验结果
- `python E:/tmp/check_indent.py .` → `--- 0 mismatches`
- `python E:/tmp/verify_comments_only.py .` → `checked 234 java files, 234 with comment changes, 0 problems`
- `mvn -o -q test-compile`（JDK 8）→ `=== FULL BUILD OK ===`（21 模块全绿）
- 全仓库 `.java` 文件中 `@author`/`@since`/`@version`/`@date` → 0 命中
- 全仓库 `.java` 文件中 U+FFFD 乱码 → 0 命中
- `git status` → 仅剩未跟踪的 `.idea/`、`docs/`

### 9.3 与本设计文档的偏差（须知悉）
本设计的「brainstorming → 书面设计 → 用户批准 → 再写实施计划」流程中，
**用户批准这一步是在 ch02…snippet 全部实现并提交之后才回到桌面的**。
也就是说，实现先行于批准。后续如需继续，顺序应为：
1. 用户复核并批准本设计文档；
2. 对第九节列出的待决项作出裁决；
3. 再用 `superpowers:writing-plans` 写实施计划（用于**补做**未覆盖的部分，
   例如非 Java 文件的注释、或对已提交内容的返工）。

## 十、待用户裁决的开放问题

以下三条来自设计文档的 builder-check，只有用户能定，且答案会改变后续要做什么：

1. **覆盖范围是否延伸到非 Java 文件？**
   仓库里还有 `.scala` 9 个、`.pig` 10 个、`.hive` 14 个、`.avsc` 18 个、
   `.py` 17 个、`.r` 1 个。本次**一律未处理**。
   - 若要继续：需要为每种语言分别定注释规范（Avro schema 注释、SQL 注释、
     Pig/Spark 的语义不同），工作量与 Java 部分相当或更大。
   - 若不要：本任务即告一段落。

2. **琐碎的 getter/setter 与 `toString`/`equals`/`hashCode` 要不要完整 Javadoc？**
   本次对 public 方法**一律**写了完整 Javadoc，包括这些样板方法。
   - 保持现状：注释量最大，但每处都一致。
   - 改为豁免：样板方法只留一行 `//` 说明，Javadoc 只留给有业务含义的方法——
     更干净，但需返工若干已提交的模块。

3. **`@author`/`@since` 要不要写？**
   `java-comment` skill 的名义要求里提到过，但上游代码风格没有、
   且本次 18 个 commit 全部未写。
   - 保持现状（不写）：与上游风格一致，噪音最小。
   - 补写：需要批量给 234 个文件加标签——且作者名该写谁本身也需要用户指定。
