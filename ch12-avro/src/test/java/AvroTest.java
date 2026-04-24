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
 * Avro 数据序列化与反序列化测试类。
 *
 * <p>本测试类全面演示了 Avro 的核心功能：
 * <ul>
 *   <li>基本类型 (int, string) 的序列化与反序列化</li>
 *   <li>GenericRecord 和 SpecificRecord 的使用</li>
 *   <li>DataFile 的读写操作</li>
 *   <li>Schema 解析与验证</li>
 *   <li>Schema 兼容性与投影 (Schema Resolution)</li>
 * </ul>
 *
 * <p>这些测试展示了《Hadoop: The Definitive Guide》书中关于 Avro 的主要概念。
 */
public class AvroTest {

    /**
     * 测试基本 int 类型的序列化与反序列化。
     *
     * <p>演示 Avro 二进制编码的基本流程:
     * <ul>
     *   <li>使用 GenericDatumWriter 将数据编码为二进制</li>
     *   <li>使用 GenericDatumReader 解码二进制数据</li>
     * </ul>
     */
    @Test
    public void testInt() throws IOException {
        Schema schema = new Schema.Parser().parse("\"int\"");

        int datum = 163;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        writer.write(datum, encoder); // boxed
        encoder.flush();
        out.close();

        // 注意: 读取时需要提供 schema，因为二进制流中不包含 schema 信息
        DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema);
        Decoder decoder = DecoderFactory.get()
          .binaryDecoder(out.toByteArray(), null /* reuse */);
        Integer result = reader.read(null /* reuse */, decoder);
        assertThat(result, is(163));

        // 测试读取完毕后的行为 - 应该抛出 EOFException
        try {
          reader.read(null, decoder);
          fail("Expected EOFException");
        } catch (EOFException e) {
          // expected
        }
    }

    /**
     * 测试字符串类型的序列化与反序列化。
     *
     * <p>使用 String 类型 (而非 Avro 默认的 Utf8) 的配置。
     */
    @Test
    public void testGenericString() throws IOException {
        Schema schema = new Schema.Parser().parse("{\"type\": \"string\", \"avro.java.string\": \"String\"}");

        String datum = "foo";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<String> writer = new GenericDatumWriter<String>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        writer.write(datum, encoder); // boxed
        encoder.flush();
        out.close();

        DatumReader<String> reader = new GenericDatumReader<String>(schema);
        Decoder decoder = DecoderFactory.get()
          .binaryDecoder(out.toByteArray(), null /* reuse */);
        String result = reader.read(null /* reuse */, decoder);
        assertThat(result, equalTo("foo"));

        try {
          reader.read(null, decoder);
          fail("Expected EOFException");
        } catch (EOFException e) {
          // expected
        }
    }

    /**
     * 测试 GenericRecord 的创建、序列化与反序列化。
     *
     * <p>演示 Generic API 的使用流程:
     * <ul>
     *   <li>从 .avsc 文件解析 Schema</li>
     *   <li>创建 GenericRecord 并设置字段值</li>
     *   <li>序列化为二进制</li>
     *   <li>反序列化回 GenericRecord</li>
     * </ul>
     */
    @Test
    public void testPairGeneric() throws IOException {
// vv AvroParseSchema
        // 从资源文件解析 Avro Schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(
          getClass().getResourceAsStream("StringPair.avsc"));
// ^^ AvroParseSchema

// vv AvroGenericRecordCreation
        // 创建 GenericRecord (通用记录，不需要代码生成)
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
// ^^ AvroGenericRecordCreation

// vv AvroGenericRecordSerialization
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer =
          new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
// ^^ AvroGenericRecordSerialization

// vv AvroGenericRecordDeserialization
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
     * 测试 SpecificRecord (代码生成) 的序列化与反序列化。
     *
     * <p>Specific API 使用编译时生成的类 (如 StringPair)，
     * 提供更好的类型安全性和性能。
     *
     * <p>与 Generic API 对比:
     * <ul>
     *   <li>使用 StringPair.setLeft() / getLeft() 方法</li>
     *   <li>使用 SpecificDatumReader/Writer</li>
     *   <li>编译时类型检查</li>
     * </ul>
     */
    @Test
    public void testPairSpecific() throws IOException {

// vv AvroSpecificStringPair
        // 使用编译时生成的 StringPair 类
        /*[*/StringPair datum = new StringPair();
        datum.setLeft("L");
        datum.setRight("R");/*]*/

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // 使用 SpecificDatumWriter 处理特定类型
        /*[*/DatumWriter<StringPair> writer =
          new SpecificDatumWriter<StringPair>(StringPair.class);/*]*/
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        // 使用 SpecificDatumReader 读取
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
     * 测试 Avro DataFile 的创建和读取。
     *
     * <p>DataFile 是 Avro 的自描述数据格式:
     * <ul>
     *   <li>文件头包含 Schema 信息</li>
     *   <li>数据块存储在文件中</li>
     *   <li>支持压缩</li>
     *   <li>可以追加写入</li>
     * </ul>
     */
    @Test
    public void testDataFile() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));

        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

// vv AvroDataFileCreation
        File file = new File("data.avro");
        DatumWriter<GenericRecord> writer =
          new GenericDatumWriter<GenericRecord>(schema);
        // DataFileWriter 支持追加写入
        DataFileWriter<GenericRecord> dataFileWriter =
          new DataFileWriter<GenericRecord>(writer);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(datum);
        dataFileWriter.close();
// ^^ AvroDataFileCreation

// vv AvroDataFileGetSchema
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<GenericRecord>(file, reader);
        // 可以从文件中获取写入时的 Schema
        assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
// ^^ AvroDataFileGetSchema

// vv AvroDataFileRead
        assertThat(dataFileReader.hasNext(), is(true));
        GenericRecord result = dataFileReader.next();
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        assertThat(dataFileReader.hasNext(), is(false));
// ^^ AvroDataFileRead

        file.delete();
    }

    /**
     * 测试 DataFile 的迭代读取 (带复用对象)。
     *
     * <p>DataFileReader.next(record) 可以复用已存在的对象，
     * 减少内存分配。
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
        datum.put("right", new Utf8("r")); // 修改第二个字段
        dataFileWriter.append(datum);
        dataFileWriter.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<GenericRecord>(file, reader);
        assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));

// vv AvroDataFileIterator
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
          record = dataFileReader.next(record); // 复用对象
          // process record
        }
// ^^ AvroDataFileIterator

        // 再次读取，统计记录数
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
     * 测试 DataFile 的增强型 for 循环迭代。
     *
     * <p>Avro DataFileReader 支持 Iterable 接口，
     * 可以使用增强型 for 循环遍历。
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
     * 测试 Schema 解析 (Schema Resolution)。
     *
     * <p>当读取数据的 Schema 与写入数据的 Schema 不完全一致时，
     * Avro 可以处理这种情况:
     * <ul>
     *   <li>读取 Schema 可以是写入 Schema 的超集</li>
     *   <li>新字段可以有默认值</li>
     *   <li>字段顺序可以不同</li>
     * </ul>
     *
     * <p>本测试展示写入 Schema (StringPair) 与读取 Schema (NewStringPair) 的差异:
     * NewStringPair 多了一个有默认值的 description 字段。
     */
    @Test
    public void testSchemaResolution() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        GenericRecord datum = new GenericData.Record(schema); // 无 description 字段
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

// vv AvroSchemaResolution
        // 指定写入 Schema 和读取 Schema，Avro 会自动处理差异
        DatumReader<GenericRecord> reader =
          /*[*/new GenericDatumReader<GenericRecord>(schema, newSchema);/*]*/
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(),
          null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        // description 字段有默认值 ""
        /*[*/assertThat(result.get("description").toString(), is(""));/*]*/
// ^^ AvroSchemaResolution
    }

    /**
     * 测试带别名的 Schema 解析。
     *
     * <p>当字段名称改变但语义相同时，
     * 可以使用别名 (aliases) 来建立映射关系。
     *
     * <p>写入时使用 left/right 字段，
     * 读取时使用 first/second 字段 (通过别名映射)。
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

        // 使用 AliasedStringPair 的 first/second 字段读取 left/right 数据
        DatumReader<GenericRecord> reader =
          new GenericDatumReader<GenericRecord>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("first").toString(), is("L"));
        assertThat(result.get("second").toString(), is("R"));

        // 旧字段名称不再可用
        assertThat(result.get("left"), is((Object) null));
        assertThat(result.get("right"), is((Object) null));
    }

    /**
     * 测试 Schema 解析中的 null 联合类型处理。
     *
     * <p>当新 Schema 中的字段使用联合类型 [null, string]，
     * 且默认值为 null 时，
     * 旧数据中不存在的字段会返回 null。
     */
    @Test
    public void testSchemaResolutionWithNull() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPairWithNull.avsc"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

        // write schema, read schema
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        // description 字段为 null 类型，默认值为 null
        assertThat(result.get("description"), is((Object) null));
    }

    /**
     * 测试不兼容 Schema 的处理。
     *
     * <p>当写入 Schema 和读取 Schema 完全不兼容时
     * (例如写入 record 类型，读取 array 类型)，
     * 会抛出 AvroTypeException。
     */
    @Test
    public void testIncompatibleSchemaResolution() throws IOException {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
        Schema newSchema = new Schema.Parser().parse("{\"type\": \"array\", \"items\": \"string\"}");

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");
        writer.write(datum, encoder);
        encoder.flush();

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        try {
          reader.read(null, decoder);
          fail("Expected AvroTypeException");
        } catch (AvroTypeException e) {
          // expected - Schema 类型不兼容
        }

    }

    /**
     * 测试 DataFile 中的 Schema 解析。
     *
     * <p>DataFile 本身包含 Schema 信息，
     * 但读取时仍可指定不同的 Schema 进行投影。
     *
     * <p>当 reader 的 writer schema 为 null 时，
     * 表示使用 DataFile 中存储的 Schema。
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
        // writer schema 为 null，使用 DataFile 中存储的 schema
        DatumReader<GenericRecord> reader =
          new GenericDatumReader<GenericRecord>(/*[*/null/*]*/, newSchema);
// ^^ AvroSchemaResolutionWithDataFile
        DataFileReader<GenericRecord> dataFileReader =
          new DataFileReader<GenericRecord>(file, reader);
        // schema 是实际写入时的 schema (不是读取 schema)
        assertThat(schema, is(dataFileReader.getSchema()));

        assertThat(dataFileReader.hasNext(), is(true));
        GenericRecord result = dataFileReader.next();
        assertThat(result.get("left").toString(), is("L"));
        assertThat(result.get("right").toString(), is("R"));
        assertThat(result.get("description").toString(), is("")); // 新字段有默认值
        assertThat(dataFileReader.hasNext(), is(false));

        file.delete();
    }

}