// == AvroParseSchema
// == AvroGenericRecordCreation
// == AvroGenericRecordSerialization
// == AvroGenericRecordDeserialization
// == AvroSpecificStringPair
// == AvroDataFileCreation
// == AvroDataFileGetSchema
// == AvroDataFileRead
// == AvroDataFileIterator
// == AvroDataFileShortIterator
// == AvroSchemaResolution
// == AvroSchemaResolutionWithDataFile
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Avro 各功能的单元测试
 *
 * <p>这些用例覆盖了第 12 章讲到的 Avro 基础：
 * 模式的解析、通用记录的创建与序列化、具体记录的用法、
 * Avro 容器文件（DataFile）的读写与迭代，以及模式演进
 * <p>多数用例以 {@code StringPair.avsc} 这份模式为输入，
 * 它有两个字符串字段：left 与 right
 */
public class AvroTest {

  /**
   * 把一个整数写进内存流，再读回来
   *
   * <p>Avro 的编解码分两步：
   * 先用 {@link Encoder} 把对象变成字节，
   * 再用 {@link Decoder} 把字节变回对象
   * <p>注意模式并不写在数据流里，
   * 所以读的时候必须把同一个模式再告诉读取器
   */
  @Test
  public void testInt() throws IOException {
    // 模式就是一个 "int"，表示一个整数
    Schema schema = new Schema.Parser().parse("\"int\"");
    
    // 要写出去的数据
    int datum = 163;

    // 内存输出流与写出器：写出器按模式把对象编码成字节
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
    // 编码器把字节写进内存流，写出器再把数据写进去
    // 这里传的是 int，会被自动装箱成 Integer
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    // 写出数据后要冲刷缓冲区，确保字节都写出去
    writer.write(datum, encoder); // boxed
    encoder.flush();
    out.close();
    
    // 读取器也要知道模式：数据流里并没有带着它
    // 解码器负责把字节还原成对象
    DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema); // have to tell it the schema - it's not in the data stream!
    Decoder decoder = DecoderFactory.get()
      .binaryDecoder(out.toByteArray(), null /* reuse */);
    // 读出一个整数；第一个参数为 null 表示不复用对象
    Integer result = reader.read(null /* reuse */, decoder);
    // 断言读回来的值与写出去的一致
    assertThat(result, is(163));
    
    try {
      // 数据已经读完，再读一次应该抛 EOFException
      reader.read(null, decoder);
      fail("Expected EOFException");
    } catch (EOFException e) {
      // expected
    }
  }
  
  /**
   * 把一个字符串写进内存流，再读回来
   *
   * <p>模式里的 {@code "avro.java.string"} 指定了
   * 用 Java 的 String 而不是 Avro 默认的 Utf8 来表示字符串
   */
  @Test
  public void testGenericString() throws IOException {
    // "avro.java.string" 让 Avro 用 Java 的 String 类型
    Schema schema = new Schema.Parser().parse("{\"type\": \"string\", \"avro.java.string\": \"String\"}");
    
    // 要写出去的数据
    String datum = "foo";

    // 内存输出流与写出器：写出器按模式把对象编码成字节
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<String> writer = new GenericDatumWriter<String>(schema);
    // 编码器把字节写进内存流，写出器再把数据写进去
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    // 写出数据后要冲刷缓冲区，确保字节都写出去
    writer.write(datum, encoder); // boxed
    encoder.flush();
    out.close();
    
    // 读取器同样需要模式
    // 解码器负责把字节还原成字符串
    DatumReader<String> reader = new GenericDatumReader<String>(schema);
    Decoder decoder = DecoderFactory.get()
      .binaryDecoder(out.toByteArray(), null /* reuse */);
    // 读出字符串
    String result = reader.read(null /* reuse */, decoder);
    // 断言读回来的值与写出去的一致
    assertThat(result, equalTo("foo"));
    
    try {
      // 数据已经读完，再读一次应该抛 EOFException
      reader.read(null, decoder);
      fail("Expected EOFException");
    } catch (EOFException e) {
      // expected
    }
  }
  
  /**
   * 用通用写法（generic）读写一条记录
   *
   * <p>通用写法不需要生成 Java 类：
   * 记录用 {@link GenericRecord} 表示，字段按名字存取
   */
  @Test
  public void testPairGeneric() throws IOException {
// vv AvroParseSchema
    // 解析器：Avro 用它把模式文本（JSON）变成 Schema 对象
    Schema.Parser parser = new Schema.Parser();
    // 从测试资源里读出模式文件并解析
    // 模式文件就在 classpath 下，不必依赖文件系统
    Schema schema = parser.parse(
      getClass().getResourceAsStream("StringPair.avsc"));
// ^^ AvroParseSchema
    
// vv AvroGenericRecordCreation
    // 通用记录：字段按名字存取，不需要为模式生成专门的类
    GenericRecord datum = new GenericData.Record(schema);
    // 按字段名填值：字段名必须与模式里写的一致
    datum.put("left", "L");
    datum.put("right", "R");
// ^^ AvroGenericRecordCreation
    
// vv AvroGenericRecordSerialization
    // 序列化：写出器按模式把记录编码成字节
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer =
      new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
// ^^ AvroGenericRecordSerialization
    
// vv AvroGenericRecordDeserialization
    // 反序列化：读取器按同一个模式把字节还原成记录
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
      null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
// ^^ AvroGenericRecordDeserialization
  }

  /**
   * 用具体写法（specific）读写一条记录
   *
   * <p>具体写法由模式生成专门的 Java 类，
   * 字段用 getter/setter 访问，编译期就能发现字段名写错
   */
  @Test
  public void testPairSpecific() throws IOException {
    
// vv AvroSpecificStringPair
    // 具体记录：字段由生成的类保证类型与名字都正确
    /*[*/StringPair datum = new StringPair();
    datum.setLeft("L");
    datum.setRight("R");/*]*/

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    /*[*/DatumWriter<StringPair> writer =
      new SpecificDatumWriter<StringPair>(StringPair.class);/*]*/
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    /*[*/DatumReader<StringPair> reader =
      new SpecificDatumReader<StringPair>(StringPair.class);/*]*/
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
      null);
    StringPair result = reader.read(null, decoder);
    assertThat(result./*[*/getLeft()/*]*/, is("L"));
    assertThat(result./*[*/getRight()/*]*/, is("R"));
// ^^ AvroSpecificStringPair
  }

  /**
   * 写一个 Avro 容器文件，再读回来
   *
   * <p>容器文件（DataFile）在数据之外还带一份模式，
   * 因此读的时候不必另外提供模式
   */
  @Test
  public void testDataFile() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

// vv AvroDataFileCreation
    // 创建容器文件：模式会随文件一起保存下来
    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer =
      new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    dataFileWriter.close();
// ^^ AvroDataFileCreation
    
// vv AvroDataFileGetSchema
    // 读回文件里附带的模式
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
// ^^ AvroDataFileGetSchema
    
// vv AvroDataFileRead
    // 逐条读取文件里的记录
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(dataFileReader.hasNext(), is(false));
// ^^ AvroDataFileRead
    
    file.delete();
  }
  
  /**
   * 用迭代器的方式遍历容器文件
   *
   * <p>迭代器可以复用一个记录对象，
   * 因此处理大量记录时不必反复新建对象
   */
  @Test
  public void testDataFileIteration() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    datum.put("right", new Utf8("r"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

// vv AvroDataFileIterator
    // 迭代器：next(记录) 会复用传入的那个对象
    GenericRecord record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      // process record
    }
// ^^ AvroDataFileIterator

    dataFileReader =
        new DataFileReader<GenericRecord>(file, reader);
    int count = 0;
    record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      // process record
      count++;
      assertThat(record.get("left").toString(), is("L"));
      if (count == 1) {
        assertThat(record.get("right").toString(), is("R"));
      } else {
        assertThat(record.get("right").toString(), is("r"));        
      }
    }

    assertThat(count, is(2));
    file.delete();
  }
  
  /**
   * 用 for-each 的方式遍历容器文件
   *
   * <p>这是 {@link DataFileReader} 实现 {@link Iterable} 带来的写法，
   * 代码比手写 while 循环简洁
   */
  @Test
  public void testDataFileIterationShort() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    datum.put("right", new Utf8("r"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

// vv AvroDataFileShortIterator
    // 直接把读取器当成集合来遍历
    for (GenericRecord record : dataFileReader) {
      // process record
    }
// ^^ AvroDataFileShortIterator

    dataFileReader =
        new DataFileReader<GenericRecord>(file, reader);
    int count = 0;
    for (GenericRecord record : dataFileReader) {
      // process record
      count++;
      assertThat(record.get("left").toString(), is("L"));
      if (count == 1) {
        assertThat(record.get("right").toString(), is("R"));
      } else {
        assertThat(record.get("right").toString(), is("r"));        
      }
    }

    assertThat(count, is(2));
    file.delete();
  }
  
  /**
   * 读数据时用新的模式：模式演进
   *
   * <p>写数据用旧模式、读数据用新模式，
   * Avro 会按名字把旧字段映射到新模式上
   */
  @Test
  public void testSchemaResolution() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
// vv AvroSchemaResolution
    // 读取器同时拿到写出模式与读入模式，按名字做映射
    DatumReader<GenericRecord> reader =
      /*[*/new GenericDatumReader<GenericRecord>(schema, newSchema);/*]*/
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
      null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    /*[*/assertThat(result.get("description").toString(), is(""));/*]*/
// ^^ AvroSchemaResolution
  }
  
  /**
   * 用别名完成模式演进
   *
   * <p>新模式里用别名声明旧字段名，
   * 于是 left/right 也能映射到 first/second
   */
  @Test
  public void testSchemaResolutionWithAliases() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("AliasedStringPair.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema, newSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    // 别名生效了：值读到了新字段 first
    assertThat(result.get("first").toString(), is("L"));
    // 别名生效了：值读到了新字段 second
    assertThat(result.get("second").toString(), is("R"));

    // old field names don't work
    // 但旧字段名本身取不到了
    assertThat(result.get("left"), is((Object) null));
    // 右边同理
    assertThat(result.get("right"), is((Object) null));
  }
  
  /**
   * 新模式里多出一个带默认值的字段
   *
   * <p>多出来的字段在旧数据里没有，
   * 读的时候按新模式的默认值填上
   */
  @Test
  public void testSchemaResolutionWithNull() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPairWithNull.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    // 新模式的 description 允许为空，于是读出来就是 null
    assertThat(result.get("description"), is((Object) null));
  }
  
  /**
   * 模式完全不兼容时应当报错
   *
   * <p>旧模式是 record，新模式是 array，两者无法映射，
   * Avro 会抛出 {@link AvroTypeException}
   */
  @Test
  public void testIncompatibleSchemaResolution() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse("{\"type\": \"array\", \"items\": \"string\"}");
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    // 试着用新模式读旧数据
    // 正常情况下这里会抛 AvroTypeException
    try {
      reader.read(null, decoder);
      // 没有抛异常就说明行为不对
      fail("Expected AvroTypeException");
    // 预期的异常：模式不兼容
    } catch (AvroTypeException e) {
      // expected
    }

  }
  
  /**
   * 容器文件里的模式演进
   *
   * <p>容器文件自带写出时的模式，
   * 读的时候再给一个读入模式做演进
   */
  @Test
  public void testSchemaResolutionWithDataFile() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));
    
    File file = new File("data.avro");
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
// vv AvroSchemaResolutionWithDataFile
    // 读取器：第一个参数留空表示「模式在文件里」
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(/*[*/null/*]*/, newSchema);
// ^^ AvroSchemaResolutionWithDataFile
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat(schema, is(dataFileReader.getSchema())); // schema is the actual (write) schema
    
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description").toString(), is(""));
    assertThat(dataFileReader.hasNext(), is(false));
    
    file.delete();
  }

}
