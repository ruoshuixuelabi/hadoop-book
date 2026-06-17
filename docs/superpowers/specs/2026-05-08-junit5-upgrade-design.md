# JUnit 5 升级设计文档

## 目标

将项目中所有模块从 JUnit 4 升级到 JUnit 5（最新稳定版 5.11.6）。

## 现状

- **book/pom.xml**: `junit:junit:4.11` 定义在 `dependencyManagement`
- **12 个子模块**: ch02, ch03, ch05, ch06, ch08, ch09, ch12, ch13, ch16, ch18, ch19, common, snippet
- **39 个测试文件**: 使用 JUnit 4 import（`org.junit.Test`, `org.junit.Assert.*` 等）

## 变更范围

### 1. 依赖管理 - book/pom.xml

**新增**:
- `org.junit:junit-bom:5.11.6` (BOM，管理 JUnit 5 依赖版本)
- `org.junit.jupiter:junit-jupiter-api:5.11.6`

**移除**:
- `junit:junit:4.11`
- `org.hamcrest:hamcrest-all:1.3`
- `junit-addons:junit-addons:1.4`

**修改**:
- `maven-surefire-plugin` 升级到 `3.0.0+`（支持 JUnit 5）

### 2. 各模块 POM 更新

将所有模块中的:
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

### 3. 测试代码迁移 (39 个文件)

| JUnit 4 | JUnit 5 |
|---------|---------|
| `import org.junit.Test` | `import org.junit.jupiter.api.Test` |
| `import org.junit.Assert.*` | `import org.junit.jupiter.api.Assertions.*` |
| `import org.junit.Assume.*` | `import org.junit.jupiter.api.Assumptions.*` |
| `@Before` | `@BeforeEach` |
| `@After` | `@AfterEach` |
| `@BeforeClass` | `@BeforeAll` |
| `@AfterClass` | `@AfterAll` |
| `@Ignore` | `@Disabled` |
| `org.junit.matchers.JUnitMatchers.*` | 无对应（简化处理） |

### 4. snippet/pom.xml 特殊处理

snippet 模块额外引用了 `junit-addons`，移除该依赖。

## 实施步骤

1. 更新 `book/pom.xml` 中的 `dependencyManagement` 和 `surefire-plugin`
2. 更新所有 13 个子模块的 pom.xml
3. 批量替换所有测试文件的 import 语句
4. 运行 `mvn test` 验证

## 回滚方案

如遇问题，通过 `git checkout` 回滚 POM 和测试文件。
