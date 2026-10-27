# hadoop-book 中文教学注释化设计文档

日期：2026-10-26
状态：**第一阶段已实现并提交**（详见第九节「实施结果」）；**第二阶段（非 Java 文件）规范已就绪、尚未落笔**（详见第十一节）

## 一、意图与背景

`hadoop-book` 是《Hadoop: The Definitive Guide, Fourth Edition》的示例代码仓库，
当前处于上游 `tomwhite/hadoop-book` 的 `upstream/update`（d72fad4, 2017-10-13）纯净状态。

用户正在系统学习 Hadoop，需要读懂每一章的示例代码。当前仓库里
**234 个 Java 文件、约 17,000 行代码，注释几乎为零**（全仓库仅 18 个文件含 Javadoc 块）。

本次工作的目标：为全部 Java 源码补充中文教学注释，使读者能脱离书籍正文独立读懂代码。

用户后续进一步裁决：**覆盖范围延伸到全部非 Java 文件**（153 个），
每种语言的注释语法与风险对策见第十一节。

**为什么是"只加注释"**：上一轮 AI 曾经尝试过大规模改写这个仓库，但同时做了
注释添加、代码格式化和依赖升级三件事，结果破坏性变更与注释混在一起无法区分，
最终被整体回滚。本次严格限定：**只允许新增/修改注释，源码逻辑一行不动**。

## 二、目标与非目标

### 目标
- 全部 Java 源码（`src/main/java` + `src/test/java`）覆盖中文教学注释
- 全部非 Java 源文件（153 个）覆盖中文教学注释（第二阶段，规则见第十一节）
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
| 覆盖范围（第一阶段） | `src/main/java` 全部 + `src/test/java` 全部（含 `oldapi/`） |
| 覆盖范围（第二阶段） | **全部非 Java 文件**（153 个，见第十一节清单）；`pom.xml` 与 `.gitignore` 排除 |
| 注释密度 | 教学型（类 Javadoc + public 方法完整 Javadoc + 关键步骤行内注释） |
| 落地方式 | 每个模块一个 commit，直接提交到 master |
| 推进顺序 | 按书章节顺序 ch02 → ch03 → ch05 → … |
| 样板方法 Javadoc | **保持现状：全部写**（getter/setter/toString/equals/hashCode 也写完整 Javadoc） |
| `@author`/`@since` | **不写，保持现状** |

### 3.7 实施中确定下来的细则
以下条目在实施过程中被具体化并全程遵守：

| 项 | 细则 |
|---|---|
| 覆盖范围（第一阶段） | `src/main/java` + `src/test/java` 全部 Java 文件，**含 `common/src/main/java/oldapi/` 下的旧 API 副本**。非 Java 文件的处理规则见第十一节（第二阶段） |
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

### 9.4 第二阶段（非 Java 文件）的准备状态
第十一节的规范已就绪并**已用实测证据支撑**，但**尚未开始落笔**。
已完成的准备工作：

| 事项 | 状态 |
|---|---|
| 153 个文件的最终清单 | ✅ `E:/tmp/nonjava_final.txt`（全部为 git 已跟踪文件） |
| 每种语言的注释语法 | ✅ 见 §11.4–§11.7 |
| Hive 只能用 `--` 的依据 | ✅ `HiveLexer.g` 486–489 行（无 `#` token）+ `CliDriver.java` 的跳过逻辑 |
| Avro `.avsc` 用 `//` 的依据 | ✅ 插件层 BUILD SUCCESS + 生成层 `SCHEMA$` 未变 + 12/12 测试通过 + `Schema.java` 的 `ALLOW_COMMENTS` |
| Python 2/3 分类与 PEP 263 规则 | ✅ 逐文件 `py_compile` 实测：12 个 py2 / 5 个 py3 |
| Scala 中文注释可编译 | ✅ `mvn -o -pl ch19-spark clean test-compile` BUILD SUCCESS |
| shell 语法校验 | ✅ 全部 tracked `.sh` 过 `bash -n`，0 失败 |
| XML 良构校验 | ✅ 全部 tracked `.xml` 解析通过，0 畸形 |
| `workflow.xml` 进书稿的实测 | ✅ `perl phragmite_db.pl` 实跑，转义正确，中文透传 |
| 非 Java 的「只增不改」校验器 | ⬜ 未写（需按语言分别处理注释行剥离） |
| 各模块注释正文 | ⬜ 未写 |

## 十、已裁决的开放问题

以下三条来自设计文档的 builder-check，用户已全部裁决：

| # | 问题 | 裁决 | 落实情况 |
|---|---|---|---|
| 1 | 覆盖范围是否延伸到非 Java 文件？ | **继续，覆盖全部非 Java 文件** | 见第十一节；本阶段尚未实施 |
| 2 | 样板方法（getter/setter/toString/equals/hashCode）要不要完整 Javadoc？ | **保持现状：全部写** | 第一阶段已如此，**不返工** |
| 3 | `@author`/`@since` 要不要写？ | **不写，保持现状** | 第一阶段已如此，**不返工** |

第 2、3 条意味着第一阶段的 18 个 commit **不需要返工**；第二阶段只做增量。

## 十一、非 Java 文件的注释规范（第二阶段）

### 11.1 覆盖范围

用户裁决为「覆盖全部非 Java 文件」。经全仓库清点，符合「值得加教学注释」
的非 Java 源文件共 **153 个**，清单存于 `E:/tmp/nonjava_final.txt`。

**排除项**（共 2 个文件 + 一批目录）：
- `pom.xml`、`*/pom.xml` —— 属构建配置，见第二节「非目标」
- `.gitignore` —— 不是教学内容
- `target/`、`snippet/actual/`、`snippet/expected/` 等构建产物与生成结果
- 纯数据文件（`.txt`、`.avro`、`.parquet` 等二进制/样例输入）
- 第三方 vendored 文件：
  - `snippet/conf/local/**` 与 `snippet/conf/pseudo/**` —— 从 Apache Hadoop 发行版原样拷贝的
    集群配置（`hadoop-env.sh`、`core-site.xml`、`*.cmd`、`*.template`、`*.example` 等），
    头部带 ASF 许可证声明，属于上游文件而非本书原创；
  - `snippet/bin/phragmite_db.pl` —— 版权属于 Tim Peierls 与 Brian Goetz（2006），
    是从上游借来的片段抽取工具；
  - `ch18-crunch/src/test/resources/log4j.properties` 与
    `ch19-spark/src/test/resources/log4j.properties` —— 同样是 ASF 许可证的 vendored 文件。

**一处需要说明的边界**：`book/src/main/assembly/jar.xml`、
`ch12-avro/src/main/assembly/job.xml`、`ch13-parquet/src/main/assembly/job.xml`、
`ch18-crunch/src/main/assembly/hadoop-job.xml` 是 **Maven assembly 描述符 /
Hadoop job 模板**，形式上属于构建配置。但：
- `job.xml` / `hadoop-job.xml` 是**书里的教学内容**（MapReduce 作业模板），
  加中文注释对读者有实际价值；
- `jar.xml` 是纯构建描述符，加注释只有噪音价值。

处理方式：`jar.xml` **只加一行文件级注释**说明它打包什么，不逐段注释；
其余三个作业模板正常加教学注释。所有 XML 注释都只是 `<!-- -->`，
不影响 Maven assembly 插件的解析（assembly 描述符由 Maven 的 XML 解析器读，
注释元素会被正常忽略）。

按扩展名分布：

| 扩展名 | 数量 | 注释语法 | 说明 |
|---|---|---|---|
| `.xml` | 22 | `<!-- -->` | Hadoop 配置文件、Oozie workflow、assembly 描述符 |
| `.sh` | 22 | `#` | 驱动脚本、build 脚本、书稿抽取脚本；含 2 个 `*.java.pre.sh`（书上与 Java 清单并排展示的 shell 命令） |
| `.grunt` | 18 | `--` / `/* */` | Pig grunt 脚本 |
| `.avsc` | 18 | `//`（JSON 注释） | Avro schema，见 11.4 |
| `.py` | 17 | `#` | 见 11.3 的 PEP 263 编码声明规则 |
| `.hive` | 14 | `--`（**只能用 `--`**） | 见 11.5 |
| `.properties` | 12 | `#` | Flume、Oozie、计数器命名 |
| `.pig` | 10 | `--` / `/* */` | Pig Latin 脚本 |
| `.scala` | 9 | `//` / `/* */` | Spark Scala 示例 |
| `.rb` | 2 | `#` | Ruby streaming 示例 |
| `.cfg` | 2 | `#` | ZooKeeper 配置 |
| `.r` / `.cpp` / `.c` / `.jr` / `.macro` / `.param` | 各 1 | 见 §11.7 | R 脚本、C++、C、Java-record、Pig 宏、Pig 参数文件 |
| 无扩展名（`Makefile`） | 1 | `#` | `ch02-mr-intro/src/main/cpp/Makefile`，编译 Hadoop Pipes C++ 例子 |

按模块分布（合计 153）：ch16-pig 31、ch12-avro 17、ch17-hive 16、
ch19-spark 12、conf 11、ch09-mr-features 11、snippet 10、ch14-flume 7、
ch02-mr-intro 8、ch13-parquet 4、ch10-setup 4、ch06-mr-dev 4、ch03-hdfs 4、
appc 3、ch04-yarn 2、book 2、ch05-io 2，其余（ch08/ch18/ch21/ch22 与根目录
`functional-test.sh`）各 1。

**实施顺序**：沿用第一阶段的「按书章节顺序，每模块一个 commit」，
新增/涉及非 Java 文件的模块依次为：
ch02 → ch03 → ch04 → ch05 → ch06 → ch08 → ch09 → ch10 → ch12 → ch13 → ch14 →
ch16 → ch17 → ch18 → ch19 → ch21 → ch22 → appc → book → conf → snippet →
根目录 `functional-test.sh`。

### 11.2 通用原则

第一阶段的三条形态区分在非 Java 语言里**降级为两种**（多数脚本语言没有
Javadoc 概念）：

| 形态 | Java | 脚本/配置语言 |
|---|---|---|
| 「类/单元」级说明 | 类级 Javadoc | 文件**开头**的注释块（`/* */` 或连续 `#` 行），说明这个文件是干什么的 |
| 「方法」级说明 | 方法 Javadoc | 函数/语句块**上一行**的行注释，说明这一步为什么这么做 |
| 「方法内」行内说明 | `//` 行内注释 | 行内注释（同样在上一行） |

其余原则沿用第一阶段：
- 中文为主，原有英文注释**一字不动**
- 注释解释「为什么」，不逐行翻译代码
- 只增不改：非注释内容逐字节保持原样
- 注释放在被注释代码的**上一行**，不放行尾
- UTF-8 无 BOM，LF 换行
- 不写 `@author`/`@since`/`@version`/`@date`

### 11.3 Python：PEP 263 编码声明（硬性前置）

仓库里 17 个 `.py` 文件**没有一个**带 PEP 263 编码声明，且**没有一个**含非 ASCII 字节。
Python 2 在没有编码声明时把源码按 ASCII 解析，**一旦加入中文字符就是 SyntaxError**。

实测分类（Python 3.12.7 逐个 `py_compile`）：

- **Python 2，12 个**：`ch02-mr-intro` 2 个、`ch09-mr-features` 6 个、
  `ch16-pig` 1 个、`ch17-hive` 2 个、`snippet/bin/check_manuscript.py`
- **Python 3，5 个**：`ch12-avro/src/main/py/` 2 个、
  `ch19-spark/src/main/python/MaxTemperature.py`、
  `snippet/bin/phragmite_{hive,pig}.py`

规则：
1. 12 个 Python 2 文件**必须**在文件开头加 `# -*- coding: utf-8 -*-`。
2. 其中 13 个文件（12 个 py2 + `phragmite_{hive,pig}.py`）第 1 行是
   `#!/usr/bin/env python` shebang —— PEP 263 允许声明在第 1 或第 2 行，
   因此这些文件把编码声明放在**第 2 行**（shebang 之后）。
3. 5 个 Python 3 文件不需要声明（Python 3 默认 UTF-8），但为了一致性
   也允许加在第 1 行；**实现时统一不加**，除非该文件本身需要。

> **注意**：加编码声明属于「插入一行注释」，不改变任何代码 token，
> 符合第一阶段「只增不改」的红线。

### 11.4 Avro `.avsc`：只用 `//` JSON 注释

**结论：`.avsc` 文件用 `//` 行注释，绝不写 `/* */`，绝不新增或修改 `"doc"` 字段。**

依据（均为实测，非推测）：

1. **插件层**：把 `// 中文注释测试` 注入
   `ch19-spark/src/test/avro/WeatherRecord.avsc` 与
   `ch12-avro/src/main/resources/StringPair.avsc` 后，
   `mvn -o -pl ch12-avro,ch19-spark clean test-compile` → **BUILD SUCCESS**
   （ch12 用 avro:1.8.1:schema，ch19 用 avro:1.7.6:schema）。
2. **生成层**：`ch12-avro/target/generated-sources/java/StringPair.java`
   与 `ch19-spark/target/generated-test-sources/avro/specific/WeatherRecord.java`
   里嵌的 `SCHEMA$` 字符串**逐字节未变** —— `//` 注释不会进入生成的 Java。
   顺带确认 `doc` 字段**会**被保留。
3. **运行层**：`mvn -o -pl ch12-avro test` → `Tests run: 12, Failures: 0, Errors: 0`。
4. **根因**：`avro-1.8.1-sources.jar` → `org/apache/avro/Schema.java` 第 82–89 行，
   静态初始化块里有 `FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS)`——
   Jackson 的 ALLOW_COMMENTS 才是 `//` 合法的原因。
   avro 1.7.6 经 `javap -c` 确认同样开启了该特性。

**为什么不能写 `"doc"`**：`doc` 是 schema 的**数据**，会进 `SCHEMA$`，
会进 `ch12-avro/src/main/examples/AvroSort/output/part-r-00000.avro`
的 `avro.schema` 头（该文件里已逐字包含 `SortedStringPair.avsc` 的 schema JSON），
也会改变 `ch12-avro/src/main/java/AvroSort.java:136`
（`new Schema.Parser().parse(new File(schemaFile))`）读到的语义。
一旦动了，就是**修改数据**而非**加注释**，越过红线。

`/* */` 虽在 Avro 1.8.1 下也能解析（实测 `parse(File)` 的 `.toString()` 不变），
但它是 JSON 标准之外的写法，且 1.7.6 未单独验证，**统一只用 `//`**。

运行时消费者全部走 `parse(InputStream)`：
`ch12-avro/src/test/java/AvroTest.java` 第 156/390/424/461 行、
`ch13-parquet/src/test/java/ParquetTest.java` 第 115/184/247 行。
已实测 head/middle/tail 三种插入位置的 `equals` 与 `toString` 均与原文一致。

### 11.5 Hive `.hive`：**只能用 `--`**

**结论：`.hive` 文件的注释一律写 `--`，不能写 `#`，不能写 `/* */`。**

依据：

1. **词法层**：`hive-exec-2.1.1-sources.jar` →
   `org/apache/hadoop/hive/ql/parse/HiveLexer.g` 第 486–489 行，Hive 的
   SQL 注释 token **只有** `--`：
   ```
   COMMENT
     : '--' (~('\n'|'\r'))*
       { $channel=HIDDEN; }
     ;
   ```
   在该文件里 `grep "'#'"` → **无任何 `#` token**。（0.13.1 同规则，第 452–455 行。）
2. **CLI 层**：`hive-cli-2.3.10-sources.jar` → `CliDriver.java`
   - 文件模式 `processReader(BufferedReader r)`（第 469 行）：
     `if (! line.startsWith("--")) { qsb.append(line + "\n"); }`
     —— **没有 trim**，所以只有顶格的 `--` 行才会被跳过；
   - 交互/REPL 模式 `executeDriver`（第 816 行）：
     `if (line.trim().startsWith("--")) { continue; }`
     —— **有 trim**，缩进的 `--` 行也会被跳过。
   → 两条路径对「缩进」的处理不一致，详见下面的硬约束。
3. **仓库自证**：`ch17-hive/src/main/hive/storage.hive` 第 22 行已经写着
   `-- SET avro.output.codec=snappy;`，本书自己的 Hive 脚本就在用 `--`。

> **陷阱提醒**：`.hive` 里的 `# vv` / `# ^^` / `# ==` 标记**不是 Hive 注释**，
> 它们藏在 `! echo; # vv max_temp_select;` 这样的行里 —— `!` 开头让 Hive CLI
> 把这一行交给 `ShellCmdExecutor` 执行 shell 命令，`echo;` 之后的 `# ...`
> 是 **shell 注释**，被 `phragmite_hive.py` 从 **stdout** 里正则捕获。
> 换句话说：`#` 在这里能工作，是因为它根本不是 Hive 语法。
> 新增中文注释**绝不能**用 `#`，否则 Hive 会把它当 SQL 解析并报错。

**硬约束：`.hive` 里的中文注释必须顶格写在第 0 列。**

`.hive` 脚本是用 `hive -f xxx.hive`（文件模式）跑的，走的正是上面第 469 行
**不 trim** 的那个分支：只有**顶格**的 `--` 行会被整行跳过；**缩进**的 `--` 行
会被拼进查询缓冲区，随后交给 `splitSemiColon` 切分语句。
而 `splitSemiColon`（`CliDriver.java` 第 388/422–461 行）**只对引号敏感、
对注释不敏感**——它不知道 `--` 后面是注释，于是一个注释里的
**半角分号**会把语句切成两半，一个注释里的**单引号**会让引号配对从此错位。
（这两点都已用真实的 `hive-cli-2.3.10.jar` 实测过：`-- 注释里有 ; 会被切分吗`
被切成 2 段，`  -- 缩进的注释; 含半角分号` 同样被切成 2 段。）

把注释顶格写，第 469 行的 `line.startsWith("--")` 就成立，整行被丢弃，
根本进不了查询缓冲区——这样无论注释里写什么都不可能影响 Hive 的解析。
这是本节对实现者的**唯一硬性要求**。

**配套约束：中文注释一律整行顶格写，不要写成「代码 + 行尾 `--` 注释」。**

行尾注释（`SELECT 1; -- 中文说明`）这一行整体**不**以 `--` 开头，
所以它会被原样拼进查询缓冲区；此时注释里的分号仍会把语句切成两半、
引号仍会让配对错位。既然顶格写就够安全了，就不要用行尾注释这种写法。
（仓库里已有的 `-- SET avro.output.codec=snappy;`（`storage.hive` 第 22 行）
是**顶格**的，所以它不受这条限制——分号在顶格注释里是安全的。）

**实现者自检清单（`.hive`）**：
- [ ] 没有以 `#` 开头的行；
- [ ] 每个中文注释行都是从第 0 列开始的 `--`；
- [ ] 没有「代码后面跟 `--` 注释」的行；
- [ ] 顶格 `--` 注释里可以自由使用分号、引号等字符。

### 11.6 Pig `.pig` / `.grunt` / `.macro`

Pig 支持 `--`（行注释）与 `/* */`（块注释）。本书自带两个样例作为实证：
- `ch16-pig/src/main/pig/comment_c-style.pig`：`/* ... */` 与
  `C = JOIN A BY $0, /* ignored */ B BY $1;`
- `ch16-pig/src/main/pig/comment_single_line.pig`：`-- My program`、
  `DUMP A; -- What's in A?`

约定：
- `.pig` / `.grunt` / `.macro` 统一用 `--` 单行注释（与书中既有风格一致）。
- `.pig` 的**首行已有** `-- max_temp.pig: Finds the maximum temperature by year`，
  中文补充写在它**下一行**，不覆盖原英文行。
- `.grunt` 里的 `-- ==` / `-- vv` / `-- ^^` 是**书稿标记**（见 11.8），
  新增注释**不能**以这些前缀开头。

**为什么 `.grunt`/`.pig` 源文件里的普通注释不会进入书稿片段**（已用语法级证据证明）：

1. `snippet/bin/phragmite_pig.py` 在 **stdout** 上匹配 `-- vv` / `-- ^^`，
   而这两个 token 只存在于 `.grunt` **源文件**里 ⇒ Pig 交互式控制台会把
   **读到的源码行回显到 stdout**。
2. `snippet/bin/grunter.sh` 的 sed 规则把回显格式固定下来：
   `^grunt> ` 与 `^>> ` 各自转成 `<prompt>`/`<userinput>`；
   其中 `>> ` 来自 `PigScriptParser.secondary_prompt()` 设置的续行提示符。
3. 关键一环：Pig 的语法把 `--` 注释**词法跳过**了。
   `pig-0.13.0-sources.jar` → `org/apache/pig/tools/pigscript/parser/PigScriptParser.jj`
   第 137–141 行：
   ```
   // Skip comments(single line and multiline)
   SKIP : {
      <"--"(~["\r","\n"])*>
   |  <"#!" (~["\r","\n"])*>
   |  <"/*" (~["*"])* "*" ("*" | (~["*","/"] (~["*"])* "*"))* "/">
   }
   ```
   而 `<PIG_END> TOKEN : { <PIG: ""> { matchedToken.image = image.toString(); } : DEFAULT }`
   这条产生式**只捕获 PIG 语句正文**——注释根本不会成为一个 PIG token。
4. 旁证：`snippet/expected/ch11/grunt/` 下 43 个黄金片段里**没有一行 `--`**，
   且 18 个 `.grunt` 文件中**没有任何一行 `--` 注释位于 `vv`/`^^` 窗口内**。

⇒ 结论：即使在 `vv`/`^^` 窗口内新增中文 `--` 注释，它也**不会**被 Pig 回显，
也就**不会**污染书稿片段。（Pig 交互式回显的是被解析的语句，不是原始输入行。）

### 11.7 Scala / C / C++ / Java-record / R / Ruby / shell

| 语言 | 注释语法 | 备注 |
|---|---|---|
| Scala | `//`、`/* */` | **已实测**：在 `ch19-spark/src/main/scala/MaxTemperature.scala` 注入中文 `//` 注释后，`mvn -o -pl ch19-spark clean test-compile` → BUILD SUCCESS。平台默认编码是 cp936，但 Scala 2.10.6 正确处理 UTF-8 注释。 |
| C / C++ | `//`、`/* */` | `ch12-avro/src/main/c/dump_pairs.c`、`ch02-mr-intro/src/main/cpp/max_temperature.cpp` |
| Java-record (`.jr`) | `//`、`/* */` | `ch22-case-studies/src/main/java/TrackStats.jr`；注意该模块 pom **没有** record-compiler 插件，`TrackStats.jr` 从不参与编译，改动风险为零 |
| R | `#` | `ch09-mr-features/src/main/r/temperature_distribution.r`（仅 4 行） |
| Ruby | `#` | `ch02-mr-intro/src/main/ruby/*.rb` |
| shell / awk | `#` | `awk` 脚本嵌在 shell here-doc 里，用 `#` |
| XML | `<!-- -->` | 见 11.9 |
| `.properties` | `#` | Java `.properties` 与 Flume/Oozie 配置都吃 `#` |
| `.cfg` | `#` | ZooKeeper `zoo.cfg` |
| `.param` | `#` | Pig 参数文件，**原本就用 `#`**，沿用 |

### 11.8 书稿标记冲突禁忌（按方言区分）

书稿抽取有**四套互不相同的标记方言**，新增注释必须避开对应前缀：

| 方言 | 出现位置 | 会被误判的前缀 | 备注 |
|---|---|---|---|
| Java | `snippet/bin/phragmite_db.pl` 第 183 行，正则 `^\s*//\s*(vv+|\^\^+|xx+|cc+(?:\[.*\])?|hh+|!!+|\?\?+(?:\[.*\])?|==+|--+)\s+(\S*)\s?(.*)$` | `// vv` `// ^^` `// xx` `// cc` `// hh` `// !!` `// ??` `// ==` `// --` | 任何 **Java** 注释不得以这些开头 |
| Pig | `snippet/bin/phragmite_pig.py`，在 **stdout** 上匹配 `.*--\s+vv\s+([^<]*)` | `-- vv` `-- ^^` | `.grunt` 源文件里已有大量此类标记 |
| Hive | `snippet/bin/phragmite_hive.py`，在 **stdout** 上匹配 `.*#\s+vv\s+([^<]*);` | `# vv` `# ^^` `# ==` | 这些标记在 `! echo;` 的 payload 里 |
| shell | 无仓库内消费者（抽取器在外部书稿工作区） | `: ==` `: vv` `: ^^` | `ch02 max_temp.sh`、`ch03 file.sh`/`hars.sh`、`ch21 group.sh` |

**统一红线**：所有新增注释行
1. **以中文字符开头**（`// 中文…`、`-- 中文…`、`# 中文…`、`<!-- 中文…`）；
2. **不包含**上述任一标记 token（含变体，如 `==`、`==+`、`--`）；
3. 中文注释正文里不出现 `vv`/`^^`/`cc`/`hh`/`!!`/`??` 等两字母操作符。

**`.hive` 的额外约束**：`#` 一律不用（11.5），只用 `--`。
**`.grunt` 的额外约束**：`-- ==`/`-- vv`/`-- ^^` 是既有的书稿标记行，
新增的 `--` 中文注释不能与它们混淆（以中文开头即可自然区分）。

### 11.9 XML：`<!-- -->` 与 workflow.xml 的特殊性

XML 注释用 `<!-- -->`。本仓库统一采用**单行独占**写法：

```xml
<!-- 中文说明：这一段配置的作用 -->
```

多行块注释（`<!--` 与 `-->` 分行、中间正文每行缩进）也合法，
但本仓库现有 XML 里没有先例，统一用单行形式，避免风格分裂。

**注意 `ch06-mr-dev/src/main/resources/max-temp-workflow/workflow.xml` 是唯一
会被喂给 `phragmite_db.pl` 的非 Java 文件**
（见 `snippet/bin/generate_listings.sh` 第 8 行）。因此它的 XML 注释
**会进入书稿清单片段**。这一点已实测确认：

- `phragmite_db.pl` 对**没有片段标记**的文件，把**整个文件**作为一个片段，
  片段名 = 去掉目录后的文件名再去掉 `.java` 后缀。所以 `workflow.xml`
  变成片段 `workflow.xml`，输出文件 `workflow.xml.xml`。
- 片段内容被逐行做 XML 转义：`&`→`&amp;`、`<`→`&lt;`、`>`→`&gt;`。
  实测加入 `<!-- 中文说明：这是测试 -->` 后，生成的片段首行是
  `<programlisting format="linespecific">&lt;!-- 中文说明：这是测试 --&gt;`
  —— **转义正确，DocBook 仍然是良构 XML**，中文 UTF-8 字节原样透传
  （脚本没有 `use utf8`，直接按字节写出）。
- 副作用：书稿里会把 `<!-- 中文说明 -->` 显示成**字面文本**（因为它在
  `<programlisting>` 里是转义后的正文，不是真注释）。

**结论与实施约束**：
- `workflow.xml` **可以**加中文 XML 注释，实测不破坏任何东西。
- 但由于整份文件都会进书稿，**该文件只加一行文件级 XML 注释**
  说明用途，其余保持原样，避免书稿里出现多余的中文说明。
- 同理，任何**会被列进书稿**的文件（所有 `.java`，以及这个 `.xml`），
  其注释都会出现在出版物里。这是第一阶段就已确立并被接受的取舍
  （见 §3.2 与 §八「中文注释进入书稿清单片段」一行）。

**顺带一个必须知道的度量细节**：`phragmite_db.pl` 的 `lengthCheck`
用 `MAXLINE => 83` 警告超长行，而它的正则是**按字节**匹配的
（脚本无 `use utf8`），所以**一个中文字符算 3 字节**。
加中文注释后，行更容易触发「longer than 83 characters」警告。
这只是 `warn`（写到 STDERR），**不是错误**，不影响生成结果。
但实现时仍应把中文注释写得**短**，单行控制在 30 个中文字以内。

### 11.10 验证方式（不能沿用 Java 的 token 流比对）

第一阶段的三件套（`verify_comments_only.py` / `check_indent.py` / `mvn test-compile`）
**只对 Java 有效**。第二阶段按语言分别验证：

| 语言 | 验证手段 | 已跑通 |
|---|---|---|
| shell / awk | `bash -n <file>` | ✅ 全部 tracked `.sh` 通过，0 失败 |
| XML | Python `xml.etree.ElementTree.parse` 全部 tracked `.xml` | ✅ 104 个 tracked `.xml` 全部格式良好，0 畸形 |
| Python 2 | **本机无 python2**，无法直接校验；只能靠 PEP 263 规则 + 人工复核 | ⚠️ 受限 |
| Python 3 | `python -m py_compile` | ✅ |
| Scala | `mvn -o -pl ch19-spark clean test-compile` | ✅ 已验证中文注释不破坏编译 |
| Avro | `mvn -o -pl ch12-avro,ch19-spark clean test-compile` + `mvn -o -pl ch12-avro test` | ✅ 已验证 |
| Pig / Hive / Ruby / R / C++ | **本机无 pig / hive / ruby / Rscript / g++**，无法校验 | ⚠️ 受限 |
| Pig（补充） | 即便本地装了 `pig`（ch16-pig 传递依赖解析到的是 **0.16.0**），`pig -x local < x.grunt` 在本机也会**卡死**：stderr 报 `Failed to locate the winutils binary in the hadoop binary path` → `Could not locate executable null\bin\winutils.exe`，随后 Hadoop 本地 FS 在 Windows 上停住，stdout 只输出 `grunt> ` 提示符 | ❌ 不可用作验证手段 |
| `.jr` | **无 `hadoop-record-compiler` artifact**，无法编译校验 | ⚠️ 受限 |

**不可用的工具链**：`pig`、`hive`、`ruby`、`R`/`Rscript`、`g++`/`gcc`、
`python2`、`hadoop-record-compiler`。
其中 `pig` 属于「装了也用不了」——见上表，winutils 缺失会让它卡死。
这些文件的风险控制手段是**保守注释**：只加语言本身的注释语法，
不改任何代码行，注释本身语法简单（如 `--` 单行），出错概率极低。

**Pig 的替代验证手段**（不依赖执行）：
1. **语法级**——比对 11.6 引用的 `PigScriptParser.jj` 的 `SKIP` 规则，
   确认新增行是合法的 `--` 行注释（词法层面被跳过，不会成为 PIG token）。
2. **黄金片段级**——用 `snippet/expected/ch11/grunt/` 下 43 个既有片段做回归：
   确认 `.grunt` 源文件里**新增的注释行没有落进任何 `vv`/`^^` 窗口**，
   片段内容因此不受影响。
3. **标记扫描**——用 `grep` 确认新增注释行不以 `-- ==` / `-- vv` / `-- ^^`
   开头，不会被 `phragmite_pig.py` 误当作书稿标记。

**通用兜底校验**：写一个 `verify_nonjava_comments_only.py`，对每个
非 Java 文件做「去掉注释行后与基线逐字节相等」的比对
（基线快照取自 `E:/tmp/baseline-nonjava/`）。
这比 Java 的 token 流比对更严格也更容易，因为这些语言没有「注释藏在字符串里」
的歧义（唯一例外：`.py` 的 `'''...'''` 文档字符串、`.properties` 的值，
两者在改前先逐个确认不含 `#`/`//` 之类的伪注释）。

**已知的环境性失败（不是本次引入的）**：
`mvn -o -pl ch13-parquet test` 有 6 个 error，全部是
`Failed to locate the winutils binary in the hadoop binary path`。
这是 Windows 上缺 `hadoop.dll` 的环境问题，与注释工作无关，不得当成回归。

### 11.11 不做的事

- 不改 `pom.xml`、不改任何构建配置（延续第二节「非目标」）
- 不给 `.avsc` 新增或修改 `"doc"`
- 不用 `#` 写 Hive 注释
- 不碰 `snippet/expected/`、`snippet/actual/`、`target/`
- 不给纯数据文件（样例输入、二进制输出）加注释
- 不重排已有注释，不改原有英文

