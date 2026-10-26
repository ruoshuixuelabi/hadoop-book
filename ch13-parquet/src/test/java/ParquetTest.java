import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Parquet 文件的读写测试
 *
 * <p>前一个用例直接用 Parquet 自己的示例接口读写，
 * 后三个用例改用 Avro 那套接口，
 * 并演示「投影」：只读文件中的一部分字段
 * <p>投影的好处是 Parquet 只读取需要的列，
 * 其余列的数据根本不会从磁盘上读出来
 */
public class ParquetTest {

  /**
   * 用 Parquet 自己的示例接口写一个文件再读回来
   *
   * @throws IOException 读写失败
   */
  @Test
  public void testParquetFile() throws IOException {
    // 用 Parquet 自己的模式描述两个字符串字段
    MessageType schema = MessageTypeParser.parseMessageType(
        "message Pair {\n" +
        "  required binary left (UTF8);\n" +
        "  required binary right (UTF8);\n" +
        "}");

    // 记录工厂：按模式来创建记录对象
    GroupFactory groupFactory = new SimpleGroupFactory(schema);
    // 新建一条记录并依次填入两个字段
    Group group = groupFactory.newGroup()
        .append("left", "L")
        .append("right", "R");

    // 写入用的配置
    Configuration conf = new Configuration();
    // 输出文件名
    Path path = new Path("data.parquet");
    // 写入用的支持类
    GroupWriteSupport writeSupport = new GroupWriteSupport();
    // 告诉它按哪个模式写
    GroupWriteSupport.setSchema(schema, conf);
    // 打开写入器：后面几个参数依次是压缩算法、
    // 块大小、页大小、字典页大小、是否启用字典、
    // 是否校验、写入的格式版本以及写入用的配置，
    // 这里全部用默认值
    ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
        ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE, /* dictionary page size */
        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
        ParquetProperties.WriterVersion.PARQUET_1_0, conf);
    // 写入一条记录
    writer.write(group);
    // 关闭写入器：数据会被刷到文件里
    writer.close();

    // 读取用的支持类
    GroupReadSupport readSupport = new GroupReadSupport();
    // 打开读取器
    ParquetReader<Group> reader = new ParquetReader<Group>(path, readSupport);
    // 读出记录
    Group result = reader.read();
    // 必须读到了记录
    assertNotNull(result);
    // 名为 left 的字段
    assertThat(result.getString("left", 0), is("L"));
    // 名为 right 的字段
    assertThat(result.getString("right", 0), is("R"));
    // 只有一条记录，读完就没有了
    assertNull(reader.read());

    // 取得文件系统，用来删掉临时文件
    FileSystem fs = FileSystem.get(new Configuration());
    // 用完就删掉，免得留在工作目录里
    fs.delete(path, true);
  }

  /**
   * 用 Avro 那套接口写一个 Parquet 文件再读回来
   *
   * @throws IOException 读写失败
   */
  @Test
  public void testParquetFileUsingAvro() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    // 模式直接取自第 12 章用过的 StringPair.avsc
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    // 通用记录：字段按名字存取
    GenericRecord datum = new GenericData.Record(schema);
    // 填入两个字段的值
    datum.put("left", "L");
    datum.put("right", "R");

    // 输出文件名
    Path path = new Path("data.parquet");
    // Avro 那套写入器：把模式一起传进去
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    // 写入一条记录
    writer.write(datum);
    // 关闭写入器：数据会被刷到文件里
    writer.close();

    // Avro 那套读取器
    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(path);
    // 读出记录
    GenericRecord result = reader.read();
    // 必须读到了记录
    assertNotNull(result);
    // 通用记录按字段名取值
    assertThat(result.get("left").toString(), is("L"));
    // 另一个字段
    assertThat(result.get("right").toString(), is("R"));
    // 只有一条记录，读完就没有了
    assertNull(reader.read());

    // 取得文件系统，用来删掉临时文件
    FileSystem fs = FileSystem.get(new Configuration());
    // 用完就删掉，免得留在工作目录里
    fs.delete(path, true);
  }

  /**
   * 读 Parquet 文件时只投影一部分字段
   *
   * @throws IOException 读写失败
   */
  @Test
  public void testParquetFileUsingAvroProjection() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    // 写出时用的是完整的模式
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    // 通用记录：字段按名字存取
    GenericRecord datum = new GenericData.Record(schema);
    // 填入两个字段的值
    datum.put("left", "L");
    datum.put("right", "R");

    // 输出文件名
    Path path = new Path("data.parquet");
    // Avro 那套写入器：把模式一起传进去
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    // 写入一条记录
    writer.write(datum);
    // 关闭写入器：数据会被刷到文件里
    writer.close();

    // 重新建一个解析器来读另一个模式文件
    parser = new Schema.Parser();
    // 投影模式：只有 right 字段，没有 left
    Schema projectionSchema = parser.parse(
        getClass().getResourceAsStream("ProjectedStringPair.avsc"));
    // 读取用的配置
    Configuration conf = new Configuration();
    // 指定投影：Parquet 只会去读文件里的 right 列
    AvroReadSupport.setRequestedProjection(conf, projectionSchema);
    // 把配置传进去，读取器才知道要投影
    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(conf, path);
    // 读出记录
    GenericRecord result = reader.read();
    // left 没有被投影进来，所以取不到
    assertNull(result.get("left"));
    // right 投影进来了，值还在
    assertThat(result.get("right").toString(), is("R"));
    // 只有一条记录，读完就没有了
    assertNull(reader.read());

    // 取得文件系统，用来删掉临时文件
    FileSystem fs = FileSystem.get(new Configuration());
    // 用完就删掉，免得留在工作目录里
    fs.delete(path, true);
  }

  /**
   * 投影之后再指定一个读取模式
   *
   * <p>投影决定「读文件里的哪些列」，
   * 读取模式决定「这些列读出来之后按什么结构组织」，
   * 两者可以各管一摊
   *
   * @throws IOException 读写失败
   */
  @Test
  public void testParquetFileUsingAvroProjectionAndReadSchema() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    // 写出时用的是完整的模式
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));

    // 通用记录：字段按名字存取
    GenericRecord datum = new GenericData.Record(schema);
    // 填入两个字段的值
    datum.put("left", "L");
    datum.put("right", "R");

    // 输出文件名
    Path path = new Path("data.parquet");
    // Avro 那套写入器：把模式一起传进去
    AvroParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<GenericRecord>(path, schema);
    // 写入一条记录
    writer.write(datum);
    // 关闭写入器：数据会被刷到文件里
    writer.close();

    // 重新建一个解析器来读另一个模式文件
    parser = new Schema.Parser();
    // 投影模式：只有 right 字段，没有 left
    Schema projectionSchema = parser.parse(
        getClass().getResourceAsStream("ProjectedStringPair.avsc"));
    // 再换一个解析器读第三个模式文件
    parser = new Schema.Parser();
    // 读取模式：比投影模式多一个 description 字段
    Schema readSchema = parser.parse(
        getClass().getResourceAsStream("NewStringPair.avsc"));
    // 读取用的配置
    Configuration conf = new Configuration();
    // 指定投影：Parquet 只会去读文件里的 right 列
    AvroReadSupport.setRequestedProjection(conf, projectionSchema);
    // 指定读取模式：读出来的记录按这个模式组织
    AvroReadSupport.setAvroReadSchema(conf, readSchema);
    // 把配置传进去，读取器才知道要投影
    AvroParquetReader<GenericRecord> reader =
        new AvroParquetReader<GenericRecord>(conf, path);
    // 读出记录
    GenericRecord result = reader.read();
    // left 没有被投影进来，所以取不到
    assertNull(result.get("left"));
    // right 投影进来了，值还在
    assertThat(result.get("right").toString(), is("R"));
    // description 是读取模式里多出来的字段，
    // 文件里没有它，于是按读取模式的默认值填空串
    assertThat(result.get("description").toString(), is(""));
    // 只有一条记录，读完就没有了
    assertNull(reader.read());

    // 取得文件系统，用来删掉临时文件
    FileSystem fs = FileSystem.get(new Configuration());
    // 用完就删掉，免得留在工作目录里
    fs.delete(path, true);
  }

}
