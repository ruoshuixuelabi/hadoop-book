# JUnit 5 升级实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将项目所有模块从 JUnit 4 升级到 JUnit 5 (5.11.6)，包括依赖管理和测试代码迁移。

**Architecture:** 在父 POM (book/pom.xml) 集中管理 JUnit 5 依赖，通过 BOM 统一版本，各子模块仅声明 junit-jupiter 即可继承。

**Tech Stack:** JUnit 5.11.6, Maven Surefire 3.0.0+

---

## 文件变更概览

| 操作 | 文件 |
|------|------|
| 修改 | `book/pom.xml` |
| 修改 | `ch02-mr-intro/pom.xml`, `ch03-hdfs/pom.xml`, `ch05-io/pom.xml`, `ch06-mr-dev/pom.xml`, `ch08-mr-types/pom.xml`, `ch09-mr-features/pom.xml`, `ch12-avro/pom.xml`, `ch13-parquet/pom.xml`, `ch16-pig/pom.xml`, `ch18-crunch/pom.xml`, `ch19-spark/pom.xml`, `common/pom.xml`, `snippet/pom.xml` |
| 修改 | 39 个 `*Test.java` 文件 |

---

## 任务分解

### Task 1: 更新 book/pom.xml 依赖管理

**文件:**
- 修改: `book/pom.xml:386-419` (dependencyManagement 中的 JUnit 部分)
- 修改: `book/pom.xml:561-569` (surefire-plugin 配置)

- [ ] **Step 1: 替换 JUnit 4 依赖为 JUnit 5 BOM**

将 `book/pom.xml` 第 386-397 行:
```xml
<!--
  JUnit 4
  用途: Java 单元测试框架
  scope=test: 仅在测试时使用
-->
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
    <version>4.11</version>
    <scope>test</scope>
</dependency>
```

替换为:
```xml
<!--
  JUnit 5 BOM
  用途: 统一管理 JUnit 5 所有模块版本
-->
<dependency>
    <groupId>org.junit</groupId>
    <artifactId>junit-bom</artifactId>
    <version>5.11.6</version>
    <type>pom</type>
    <scope>import</scope>
</dependency>
<!--
  JUnit Jupiter API
  用途: JUnit 5 测试框架 API
-->
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter-api</artifactId>
    <version>5.11.6</version>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 2: 移除 hamcrest 和 junit-addons 依赖**

删除 `book/pom.xml` 第 402-419 行的 `hamcrest-all` 和 `junit-addons` 依赖声明。

- [ ] **Step 3: 更新 maven-surefire-plugin 版本**

将 `book/pom.xml` 第 563 行的:
```xml
<artifactId>maven-surefire-plugin</artifactId>
<version>2.17</version>
```

替换为:
```xml
<artifactId>maven-surefire-plugin</artifactId>
<version>3.0.0</version>
```

- [ ] **Step 4: 验证 book/pom.xml 语法正确**

运行: `mvn help:effective-pom -pl book -Doutput=effective-pom.xml -q`
确认 JUnit 5 依赖已正确引入。

- [ ] **Step 5: 提交**

```bash
git add book/pom.xml
git commit -m "feat: upgrade JUnit 4 to JUnit 5.11.6 in dependencyManagement

- Add junit-bom for version management
- Replace junit:junit:4.11 with junit-jupiter-api:5.11.6
- Remove hamcrest-all and junit-addons (JUnit 4 only)
- Upgrade surefire-plugin to 3.0.0 for JUnit 5 support

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: 更新子模块 POM (13 个模块)

**文件:**
- 修改: `ch02-mr-intro/pom.xml:29-32`
- 修改: `ch03-hdfs/pom.xml:39-42`
- 修改: `ch05-io/pom.xml:51-54`
- 修改: `ch06-mr-dev/pom.xml:36-39`
- 修改: `ch08-mr-types/pom.xml:61-64`
- 修改: `ch09-mr-features/pom.xml:71-74`
- 修改: `ch12-avro/pom.xml:54-57`
- 修改: `ch13-parquet/pom.xml:41-44`
- 修改: `ch16-pig/pom.xml:25-28`
- 修改: `ch18-crunch/pom.xml:111-114`
- 修改: `ch19-spark/pom.xml:58-61`
- 修改: `common/pom.xml:53-56`
- 修改: `snippet/pom.xml:35-38` 和 `snippet/pom.xml:71-74`

**统一替换** (每个模块内):

将:
```xml
<dependency>
    <groupId>junit</groupId>
    <artifactId>junit</artifactId>
</dependency>
```

替换为:
```xml
<dependency>
    <groupId>org.junit.jupiter</groupId>
    <artifactId>junit-jupiter</artifactId>
    <scope>test</scope>
</dependency>
```

**snippet 模块额外处理**: 移除 `junit-addons` 依赖 (第 71-74 行)。

- [ ] **Step 1: 更新 ch02-mr-intro/pom.xml**

- [ ] **Step 2: 更新 ch03-hdfs/pom.xml**

- [ ] **Step 3: 更新 ch05-io/pom.xml**

- [ ] **Step 4: 更新 ch06-mr-dev/pom.xml**

- [ ] **Step 5: 更新 ch08-mr-types/pom.xml**

- [ ] **Step 6: 更新 ch09-mr-features/pom.xml**

- [ ] **Step 7: 更新 ch12-avro/pom.xml**

- [ ] **Step 8: 更新 ch13-parquet/pom.xml**

- [ ] **Step 9: 更新 ch16-pig/pom.xml**

- [ ] **Step 10: 更新 ch18-crunch/pom.xml**

- [ ] **Step 11: 更新 ch19-spark/pom.xml**

- [ ] **Step 12: 更新 common/pom.xml**

- [ ] **Step 13: 更新 snippet/pom.xml** (包括移除 junit-addons)

- [ ] **Step 14: 提交**

```bash
git add ch02-mr-intro/pom.xml ch03-hdfs/pom.xml ch05-io/pom.xml ch06-mr-dev/pom.xml ch08-mr-types/pom.xml ch09-mr-features/pom.xml ch12-avro/pom.xml ch13-parquet/pom.xml ch16-pig/pom.xml ch18-crunch/pom.xml ch19-spark/pom.xml common/pom.xml snippet/pom.xml
git commit -m "feat: replace junit:junit with junit-jupiter in all modules

All 13 modules now use JUnit 5 (junit-jupiter) instead of JUnit 4.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 3: 迁移测试文件 import (39 个文件)

**文件:** 39 个 `*Test.java` 文件

**统一替换规则**:

| 搜索 | 替换 |
|------|------|
| `import org.junit.Test;` | `import org.junit.jupiter.api.Test;` |
| `import org.junit.Assert;` | `import org.junit.jupiter.api.Assertions;` |
| `import org.junit.Assert.*;` | `import org.junit.jupiter.api.Assertions.*;` |
| `import org.junit.Assume;` | `import org.junit.jupiter.api.Assumptions;` |
| `import org.junit.Assume.*;` | `import org.junit.jupiter.api.Assumptions.*;` |
| `import org.junit.Before;` | `import org.junit.jupiter.api.BeforeEach;` |
| `import org.junit.After;` | `import org.junit.jupiter.api.AfterEach;` |
| `import org.junit.BeforeClass;` | `import org.junit.jupiter.api.BeforeAll;` |
| `import org.junit.AfterClass;` | `import org.junit.jupiter.api.AfterAll;` |
| `import org.junit.Ignore;` | `import org.junit.jupiter.api.Disabled;` |

**注解替换** (文件内搜索替换):
- `@Before` → `@BeforeEach`
- `@After` → `@AfterEach`
- `@BeforeClass` → `@BeforeAll`
- `@AfterClass` → `@AfterAll`
- `@Ignore` → `@Disabled`

**注意**: `org.junit.matchers.JUnitMatchers` 无 JUnit 5 对应，如遇到需手工处理。

- [ ] **Step 1: 批量替换 ch05-io 模块 (17 个测试文件)**

所有文件: BooleanWritableTest, BytesWritableTest, TextTest, TextPairTest, IntPairTest, IntWritableTest, VIntWritableTest, VLongWritableTest, MapWritableTest, NullWritableTest, GenericWritableTest, ObjectWritableTest, FileDecompressorTest, StringTextComparisonTest, ArrayWritableTest, MapFileSeekTest, SequenceFileSeekAndSyncTest

- [ ] **Step 2: 批量替换 ch06-mr-dev 模块 (6 个测试文件)**

- [ ] **Step 3: 批量替换 ch09-mr-features 模块 (1 个测试文件)**

- [ ] **Step 4: 批量替换 ch12-avro 模块 (1 个测试文件)**

- [ ] **Step 5: 批量替换 ch13-parquet 模块 (3 个测试文件)**

- [ ] **Step 6: 批量替换 ch18-crunch 模块 (13 个测试文件)**

- [ ] **Step 7: 批量替换 ch19-spark 模块 (1 个测试文件)**

- [ ] **Step 8: 替换 ch03-hdfs 模块 (1 个测试文件)**

- [ ] **Step 9: 提交**

```bash
git add --all '**/src/test/**/*.java'
git commit -m "refactor: migrate test imports from JUnit 4 to JUnit 5

Migrate 39 test files:
- org.junit.Test -> org.junit.jupiter.api.Test
- org.junit.Assert -> org.junit.jupiter.api.Assertions
- @Before -> @BeforeEach, @After -> @AfterEach
- @BeforeClass -> @BeforeAll, @AfterClass -> @AfterAll
- @Ignore -> @Disabled

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: 验证构建

- [ ] **Step 1: 运行 mvn test 验证**

```bash
mvn test -DskipTests=false 2>&1 | tail -50
```

预期: 所有测试通过（或仅有预期失败）

- [ ] **Step 2: 如有失败，分析并修复**

常见问题:
- 缺失 import 导致编译错误 → 补充 import
- @Disabled 测试 → 确认是否需要保留
- 静态导入问题 → 检查 Assertions.assertThat 的使用方式

---

## 自检清单

- [ ] spec 中所有需求都有对应 task
- [ ] 无 placeholder (TBD/TODO)
- [ ] 所有文件路径准确
- [ ] 版本号一致性 (5.11.6)
- [ ] surefire-plugin 3.0.0 已设置
