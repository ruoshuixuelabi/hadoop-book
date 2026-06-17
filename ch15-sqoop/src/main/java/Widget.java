// ORM class for table 'widgets'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Oct 29 09:58:23 GMT 2014
// For connector: org.apache.sqoop.manager.MySQLManager

/*
 * Widget类是Sqoop从MySQL数据库的widgets表自动生成的Java类
 *
 * 本文件是Sqoop工具自动生成的，请勿手动修改
 * 如需修改表结构或生成选项，请重新运行Sqoop命令
 *
 * 数据库表结构（widgets表）：
 * - id: INTEGER - Widget的唯一标识符，主键
 * - widget_name: VARCHAR(255) - Widget的名称
 * - price: DECIMAL(10,2) - Widget的价格
 * - design_date: DATE - 设计日期
 * - version: INTEGER - 版本号
 * - design_comment: VARCHAR(255) - 设计备注
 *
 * 本类实现的关键接口：
 * - SqoopRecord: Sqoop生成的记录基类，提供序列化/反序列化方法
 * - DBWritable: 用于从JDBC ResultSet读写数据
 * - Writable: Hadoop序列化接口，用于MapReduce数据传输
 *
 * 使用方式：
 * 1. Sqoop导入：sqoop import --table widgets --connect jdbc:mysql://...
 * 2. 在MapReduce中使用Widget对象处理导入的数据
 *
 * 本文件包含大量样板代码，这些是Sqoop自动生成的，
 * 用于处理数据库字段与Java对象之间的映射和序列化
 */

// cc Sqoop和Hadoop核心库导入
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

// Sqoop库导入 - 用于处理数据库与Hadoop之间的数据转换
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;

// Java标准库导入
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Widget数据模型类 - 对应MySQL数据库中的widgets表。
 * <p>
 * 本类由Sqoop工具自动生成，代表一个Widget实体。
 * 包含以下属性：<br/>
 * - id: 唯一标识符<br/>
 * - widget_name: Widget名称<br/>
 * - price: 价格<br/>
 * - design_date: 设计日期<br/>
 * - version: 版本号<br/>
 * - design_comment: 设计备注
 * <p>
 * 本类是Java ORM（对象关系映射）类，用于：<br/>
 * 1. 从数据库读取数据（实现DBWritable接口）<br/>
 * 2. 在Hadoop MapReduce中传输数据（实现Writable接口）<br/>
 * 3. Sqoop导入导出时的序列化/反序列化
 * <p>
 * 注意：此类是自动生成的，手动修改会在重新生成时丢失。<br/>
 * 如需修改，请更改数据库表结构或Sqoop导入选项。
 *
 * @see SqoopRecord
 * @see DBWritable
 * @see Writable
 */
public class Widget extends SqoopRecord  implements DBWritable, Writable {
    // 协议版本号，用于版本兼容检查
    private final int PROTOCOL_VERSION = 3;

    /**
     * 返回类的格式版本号。
     * Sqoop使用此方法进行版本兼容性检查。
     *
     * @return 协议版本号
     */
    public int getClassFormatVersion() { return PROTOCOL_VERSION; }

    // 当前ResultSet引用，用于大对象加载
    protected ResultSet __cur_result_set;

    // Widget的各个属性字段
    private Integer id;
    /**
     * 获取Widget的ID。
     *
     * @return Widget的唯一标识符
     */
    public Integer get_id() {
        return id;
    }
    /**
     * 设置Widget的ID。
     *
     * @param id 唯一标识符
     */
    public void set_id(Integer id) {
        this.id = id;
    }
    /**
     * 链式调用方法，设置ID并返回当前对象。
     * 方便进行 fluent API 风格的代码编写。
     *
     * @param id 唯一标识符
     * @return 当前Widget对象（用于链式调用）
     */
    public Widget with_id(Integer id) {
        this.id = id;
        return this;
    }

    private String widget_name;
    /**
     * 获取Widget的名称。
     *
     * @return Widget的名称
     */
    public String get_widget_name() {
        return widget_name;
    }
    /**
     * 设置Widget的名称。
     *
     * @param widget_name 名称
     */
    public void set_widget_name(String widget_name) {
        this.widget_name = widget_name;
    }
    /**
     * 链式调用方法，设置名称并返回当前对象。
     *
     * @param widget_name 名称
     * @return 当前Widget对象
     */
    public Widget with_widget_name(String widget_name) {
        this.widget_name = widget_name;
        return this;
    }

    private java.math.BigDecimal price;
    /**
     * 获取Widget的价格。
     *
     * @return 价格（BigDecimal类型保持精度）
     */
    public java.math.BigDecimal get_price() {
        return price;
    }
    /**
     * 设置Widget的价格。
     *
     * @param price 价格
     */
    public void set_price(java.math.BigDecimal price) {
        this.price = price;
    }
    /**
     * 链式调用方法，设置价格并返回当前对象。
     *
     * @param price 价格
     * @return 当前Widget对象
     */
    public Widget with_price(java.math.BigDecimal price) {
        this.price = price;
        return this;
    }

    private java.sql.Date design_date;
    /**
     * 获取Widget的设计日期。
     *
     * @return 设计日期
     */
    public java.sql.Date get_design_date() {
        return design_date;
    }
    /**
     * 设置Widget的设计日期。
     *
     * @param design_date 设计日期
     */
    public void set_design_date(java.sql.Date design_date) {
        this.design_date = design_date;
    }
    /**
     * 链式调用方法，设置设计日期并返回当前对象。
     *
     * @param design_date 设计日期
     * @return 当前Widget对象
     */
    public Widget with_design_date(java.sql.Date design_date) {
        this.design_date = design_date;
        return this;
    }

    private Integer version;
    /**
     * 获取Widget的版本号。
     *
     * @return 版本号
     */
    public Integer get_version() {
        return version;
    }
    /**
     * 设置Widget的版本号。
     *
     * @param version 版本号
     */
    public void set_version(Integer version) {
        this.version = version;
    }
    /**
     * 链式调用方法，设置版本号并返回当前对象。
     *
     * @param version 版本号
     * @return 当前Widget对象
     */
    public Widget with_version(Integer version) {
        this.version = version;
        return this;
    }

    private String design_comment;
    /**
     * 获取Widget的设计备注。
     *
     * @return 设计备注
     */
    public String get_design_comment() {
        return design_comment;
    }
    /**
     * 设置Widget的设计备注。
     *
     * @param design_comment 设计备注
     */
    public void set_design_comment(String design_comment) {
        this.design_comment = design_comment;
    }
    /**
     * 链式调用方法，设置设计备注并返回当前对象。
     *
     * @param design_comment 设计备注
     * @return 当前Widget对象
     */
    public Widget with_design_comment(String design_comment) {
        this.design_comment = design_comment;
        return this;
    }

    /**
     * 比较两个Widget对象是否相等。
     * <p/>
     * 比较所有字段的值
     *
     * @param o 要比较的对象
     * @return 如果所有字段都相等返回true，否则返回false
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Widget)) {
            return false;
        }
        Widget that = (Widget) o;
        boolean equal = true;
        equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
        equal = equal && (this.widget_name == null ? that.widget_name == null : this.widget_name.equals(that.widget_name));
        equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
        equal = equal && (this.design_date == null ? that.design_date == null : this.design_date.equals(that.design_date));
        equal = equal && (this.version == null ? that.version == null : this.version.equals(that.version));
        equal = equal && (this.design_comment == null ? that.design_comment == null : this.design_comment.equals(that.design_comment));
        return equal;
    }

    /**
     * 内部使用的equals方法（实现细节）。
     * <p/>
     * 比较所有字段的值
     *
     * @param o 要比较的对象
     * @return 比较结果
     */
    public boolean equals0(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Widget)) {
            return false;
        }
        Widget that = (Widget) o;
        boolean equal = true;
        equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
        equal = equal && (this.widget_name == null ? that.widget_name == null : this.widget_name.equals(that.widget_name));
        equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
        equal = equal && (this.design_date == null ? that.design_date == null : this.design_date.equals(that.design_date));
        equal = equal && (this.version == null ? that.version == null : this.version.equals(that.version));
        equal = equal && (this.design_comment == null ? that.design_comment == null : this.design_comment.equals(that.design_comment));
        return equal;
    }

    /**
     * 从JDBC ResultSet读取字段值。
     * <p/>
     * 实现DBWritable接口，用于Sqoop从数据库导入数据
     *
     * @param __dbResults JDBC结果集
     * @throws SQLException 如果数据库访问出错
     */
    public void readFields(ResultSet __dbResults) throws SQLException {
        this.__cur_result_set = __dbResults;
        // 按照字段顺序读取：id, widget_name, price, design_date, version, design_comment
        this.id = JdbcWritableBridge.readInteger(1, __dbResults);
        this.widget_name = JdbcWritableBridge.readString(2, __dbResults);
        this.price = JdbcWritableBridge.readBigDecimal(3, __dbResults);
        this.design_date = JdbcWritableBridge.readDate(4, __dbResults);
        this.version = JdbcWritableBridge.readInteger(5, __dbResults);
        this.design_comment = JdbcWritableBridge.readString(6, __dbResults);
    }

    /**
     * 内部使用的readFields方法。
     *
     * @param __dbResults JDBC结果集
     * @throws SQLException 如果数据库访问出错
     */
    public void readFields0(ResultSet __dbResults) throws SQLException {
        this.id = JdbcWritableBridge.readInteger(1, __dbResults);
        this.widget_name = JdbcWritableBridge.readString(2, __dbResults);
        this.price = JdbcWritableBridge.readBigDecimal(3, __dbResults);
        this.design_date = JdbcWritableBridge.readDate(4, __dbResults);
        this.version = JdbcWritableBridge.readInteger(5, __dbResults);
        this.design_comment = JdbcWritableBridge.readString(6, __dbResults);
    }

    /**
     * 加载大对象（Blob/Clob）。
     * <p/>
     * 本实现为空，因为widgets表没有大对象字段
     *
     * @param __loader 大对象加载器
     * @throws SQLException 如果数据库访问出错
     * @throws IOException 如果I/O操作出错
     * @throws InterruptedException 如果操作被中断
     */
    public void loadLargeObjects(LargeObjectLoader __loader)
            throws SQLException, IOException, InterruptedException {
    }

    /**
     * 内部使用的大对象加载方法。
     */
    public void loadLargeObjects0(LargeObjectLoader __loader)
            throws SQLException, IOException, InterruptedException {
    }

    /**
     * 将字段值写入PreparedStatement。
     * 实现DBWritable接口，用于Sqoop导出数据到数据库。
     *
     * @param __dbStmt 预编译的SQL语句
     * @throws SQLException 如果数据库访问出错
     */
    public void write(PreparedStatement __dbStmt) throws SQLException {
        write(__dbStmt, 0);
    }

    /**
     * 将字段值写入PreparedStatement（带偏移量）。
     * 用于批量插入时设置不同行的值。
     *
     * @param __dbStmt 预编译的SQL语句
     * @param __off 字段偏移量
     * @return 写入的字段数量
     * @throws SQLException 如果数据库访问出错
     */
    public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
        JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
        JdbcWritableBridge.writeString(widget_name, 2 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeBigDecimal(price, 3 + __off, 3, __dbStmt);
        JdbcWritableBridge.writeDate(design_date, 4 + __off, 91, __dbStmt);
        JdbcWritableBridge.writeInteger(version, 5 + __off, 4, __dbStmt);
        JdbcWritableBridge.writeString(design_comment, 6 + __off, 12, __dbStmt);
        return 6;
    }

    /**
     * 内部使用的write方法。
     *
     * @param __dbStmt 预编译的SQL语句
     * @param __off 字段偏移量
     * @throws SQLException 如果数据库访问出错
     */
    public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
        JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
        JdbcWritableBridge.writeString(widget_name, 2 + __off, 12, __dbStmt);
        JdbcWritableBridge.writeBigDecimal(price, 3 + __off, 3, __dbStmt);
        JdbcWritableBridge.writeDate(design_date, 4 + __off, 91, __dbStmt);
        JdbcWritableBridge.writeInteger(version, 5 + __off, 4, __dbStmt);
        JdbcWritableBridge.writeString(design_comment, 6 + __off, 12, __dbStmt);
    }

    /**
     * 从DataInput读取字段值。
     * 实现Writable接口，用于Hadoop序列化。
     *
     * @param __dataIn 数据输入流
     * @throws IOException 如果I/O操作出错
     */
    public void readFields(DataInput __dataIn) throws IOException {
        this.readFields0(__dataIn);  }

    /**
     * 内部使用的readFields方法。
     * 使用高效的二进制格式：
     * - null值：写入Boolean.TRUE
     * - 非null值：写入Boolean.FALSE，后跟实际数据
     *
     * @param __dataIn 数据输入流
     * @throws IOException 如果I/O操作出错
     */
    public void readFields0(DataInput __dataIn) throws IOException {
        // 读取id字段（允许为null）
        if (__dataIn.readBoolean()) {
            this.id = null;
        } else {
            this.id = Integer.valueOf(__dataIn.readInt());
        }
        // 读取widget_name字段
        if (__dataIn.readBoolean()) {
            this.widget_name = null;
        } else {
            this.widget_name = Text.readString(__dataIn);
        }
        // 读取price字段
        if (__dataIn.readBoolean()) {
            this.price = null;
        } else {
            this.price = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
        }
        // 读取design_date字段
        if (__dataIn.readBoolean()) {
            this.design_date = null;
        } else {
            this.design_date = new Date(__dataIn.readLong());
        }
        // 读取version字段
        if (__dataIn.readBoolean()) {
            this.version = null;
        } else {
            this.version = Integer.valueOf(__dataIn.readInt());
        }
        // 读取design_comment字段
        if (__dataIn.readBoolean()) {
            this.design_comment = null;
        } else {
            this.design_comment = Text.readString(__dataIn);
        }
    }

    /**
     * 将字段值写入DataOutput。
     * 实现Writable接口，用于Hadoop序列化。
     *
     * @param __dataOut 数据输出流
     * @throws IOException 如果I/O操作出错
     */
    public void write(DataOutput __dataOut) throws IOException {
        // 写入id字段
        if (null == this.id) {
            __dataOut.writeBoolean(true);  // 表示null
        } else {
            __dataOut.writeBoolean(false); // 表示非null
            __dataOut.writeInt(this.id);
        }
        // 写入widget_name字段
        if (null == this.widget_name) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, widget_name);
        }
        // 写入price字段
        if (null == this.price) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.price, __dataOut);
        }
        // 写入design_date字段
        if (null == this.design_date) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            __dataOut.writeLong(this.design_date.getTime());
        }
        // 写入version字段
        if (null == this.version) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            __dataOut.writeInt(this.version);
        }
        // 写入design_comment字段
        if (null == this.design_comment) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, design_comment);
        }
    }

    /**
     * 内部使用的write方法。
     *
     * @param __dataOut 数据输出流
     * @throws IOException 如果I/O操作出错
     */
    public void write0(DataOutput __dataOut) throws IOException {
        if (null == this.id) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            __dataOut.writeInt(this.id);
        }
        if (null == this.widget_name) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, widget_name);
        }
        if (null == this.price) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.price, __dataOut);
        }
        if (null == this.design_date) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            __dataOut.writeLong(this.design_date.getTime());
        }
        if (null == this.version) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            __dataOut.writeInt(this.version);
        }
        if (null == this.design_comment) {
            __dataOut.writeBoolean(true);
        } else {
            __dataOut.writeBoolean(false);
            Text.writeString(__dataOut, design_comment);
        }
    }

    // 默认输出分隔符：逗号分隔字段，换行分隔记录
    private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);

    /**
     * 返回默认格式的字符串表示。
     *
     * @return 逗号分隔的字符串
     */
    public String toString() {
        return toString(__outputDelimiters, true);
    }

    /**
     * 使用指定分隔符返回字符串表示。
     *
     * @param delimiters 分隔符配置
     * @return 字符串表示
     */
    public String toString(DelimiterSet delimiters) {
        return toString(delimiters, true);
    }

    /**
     * 返回字符串表示，可选择是否使用记录分隔符。
     *
     * @param useRecordDelim 是否在末尾添加记录分隔符
     * @return 字符串表示
     */
    public String toString(boolean useRecordDelim) {
        return toString(__outputDelimiters, useRecordDelim);
    }

    /**
     * 使用指定分隔符和选项返回字符串表示。
     *
     * @param delimiters 分隔符配置
     * @param useRecordDelim 是否使用记录分隔符
     * @return 格式化后的字符串
     */
    public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
        StringBuilder __sb = new StringBuilder();
        char fieldDelim = delimiters.getFieldsTerminatedBy();
        // 依次追加各字段值，使用指定分隔符分隔
        __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(widget_name==null?"null":widget_name, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":price.toPlainString(), delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(design_date==null?"null":"" + design_date, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(version==null?"null":"" + version, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(design_comment==null?"null":design_comment, delimiters));
        if (useRecordDelim) {
            __sb.append(delimiters.getLinesTerminatedBy());
        }
        return __sb.toString();
    }

    /**
     * 内部使用的toString方法。
     */
    public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
        __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(widget_name==null?"null":widget_name, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":price.toPlainString(), delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(design_date==null?"null":"" + design_date, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(version==null?"null":"" + version, delimiters));
        __sb.append(fieldDelim);
        __sb.append(FieldFormatter.escapeAndEnclose(design_comment==null?"null":design_comment, delimiters));
    }

    // 默认输入分隔符：逗号分隔字段，换行分隔记录
    private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
    private RecordParser __parser;

    /**
     * 从Text解析记录。
     * Sqoop生成的parse方法，用于将文本行解析为Widget对象。
     *
     * @param __record 逗号分隔的文本行
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(Text __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从CharSequence解析记录。
     *
     * @param __record 字符序列
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(CharSequence __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从byte数组解析记录。
     *
     * @param __record 字节数组
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(byte [] __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从char数组解析记录。
     *
     * @param __record 字符数组
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(char [] __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从ByteBuffer解析记录。
     *
     * @param __record 字节缓冲区
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(ByteBuffer __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从CharBuffer解析记录。
     *
     * @param __record 字符缓冲区
     * @throws RecordParser.ParseError 如果解析失败
     */
    public void parse(CharBuffer __record) throws RecordParser.ParseError {
        if (null == this.__parser) {
            this.__parser = new RecordParser(__inputDelimiters);
        }
        List<String> __fields = this.__parser.parseRecord(__record);
        __loadFromFields(__fields);
    }

    /**
     * 从字段列表加载数据到对象属性。
     *
     * @param fields 已解析的字段列表
     */
    private void __loadFromFields(List<String> fields) {
        Iterator<String> __it = fields.listIterator();
        String __cur_str = null;
        try {
            // 解析id字段
            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
                this.id = Integer.valueOf(__cur_str);
            }

            // 解析widget_name字段
            __cur_str = __it.next();
            if (__cur_str.equals("null")) { this.widget_name = null; } else {
                this.widget_name = __cur_str;
            }

            // 解析price字段
            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
                this.price = new java.math.BigDecimal(__cur_str);
            }

            // 解析design_date字段
            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.design_date = null; } else {
                this.design_date = java.sql.Date.valueOf(__cur_str);
            }

            // 解析version字段
            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.version = null; } else {
                this.version = Integer.valueOf(__cur_str);
            }

            // 解析design_comment字段
            __cur_str = __it.next();
            if (__cur_str.equals("null")) { this.design_comment = null; } else {
                this.design_comment = __cur_str;
            }

        } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

    /**
     * 内部使用的字段加载方法。
     */
    private void __loadFromFields0(Iterator<String> __it) {
        String __cur_str = null;
        try {
            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
                this.id = Integer.valueOf(__cur_str);
            }

            __cur_str = __it.next();
            if (__cur_str.equals("null")) { this.widget_name = null; } else {
                this.widget_name = __cur_str;
            }

            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
                this.price = new java.math.BigDecimal(__cur_str);
            }

            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.design_date = null; } else {
                this.design_date = java.sql.Date.valueOf(__cur_str);
            }

            __cur_str = __it.next();
            if (__cur_str.equals("null") || __cur_str.length() == 0) { this.version = null; } else {
                this.version = Integer.valueOf(__cur_str);
            }

            __cur_str = __it.next();
            if (__cur_str.equals("null")) { this.design_comment = null; } else {
                this.design_comment = __cur_str;
            }

        } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

    /**
     * 创建对象的深拷贝。
     * Sqoop生成的类支持clone，用于在Reducer中保存最大值。
     *
     * @return 对象的深拷贝
     * @throws CloneNotSupportedException 如果不支持克隆
     */
    public Object clone() throws CloneNotSupportedException {
        Widget o = (Widget) super.clone();
        // 特殊处理Date类型的深拷贝
        o.design_date = (o.design_date != null) ? (java.sql.Date) o.design_date.clone() : null;
        return o;
    }

    /**
     * 内部使用的clone方法。
     */
    public void clone0(Widget o) throws CloneNotSupportedException {
        o.design_date = (o.design_date != null) ? (java.sql.Date) o.design_date.clone() : null;
    }

    /**
     * 获取所有字段的Map表示。
     * 用于Sqoop的某些高级功能。
     *
     * @return 字段名到字段值的Map
     */
    public Map<String, Object> getFieldMap() {
        Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
        __sqoop$field_map.put("id", this.id);
        __sqoop$field_map.put("widget_name", this.widget_name);
        __sqoop$field_map.put("price", this.price);
        __sqoop$field_map.put("design_date", this.design_date);
        __sqoop$field_map.put("version", this.version);
        __sqoop$field_map.put("design_comment", this.design_comment);
        return __sqoop$field_map;
    }

    /**
     * 内部使用的getFieldMap方法。
     */
    public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
        __sqoop$field_map.put("id", this.id);
        __sqoop$field_map.put("widget_name", this.widget_name);
        __sqoop$field_map.put("price", this.price);
        __sqoop$field_map.put("design_date", this.design_date);
        __sqoop$field_map.put("version", this.version);
        __sqoop$field_map.put("design_comment", this.design_comment);
    }

    /**
     * 通过字段名设置字段值。
     * 用于Sqoop的某些高级功能。
     *
     * @param __fieldName 字段名
     * @param __fieldVal 字段值
     * @throws RuntimeException 如果字段不存在
     */
    public void setField(String __fieldName, Object __fieldVal) {
        if ("id".equals(__fieldName)) {
            this.id = (Integer) __fieldVal;
        }
        else    if ("widget_name".equals(__fieldName)) {
            this.widget_name = (String) __fieldVal;
        }
        else    if ("price".equals(__fieldName)) {
            this.price = (java.math.BigDecimal) __fieldVal;
        }
        else    if ("design_date".equals(__fieldName)) {
            this.design_date = (java.sql.Date) __fieldVal;
        }
        else    if ("version".equals(__fieldName)) {
            this.version = (Integer) __fieldVal;
        }
        else    if ("design_comment".equals(__fieldName)) {
            this.design_comment = (String) __fieldVal;
        }
        else {
            throw new RuntimeException("No such field: " + __fieldName);
        }
    }

    /**
     * 内部使用的setField方法。
     *
     * @param __fieldName 字段名
     * @param __fieldVal 字段值
     * @return 是否设置成功
     */
    public boolean setField0(String __fieldName, Object __fieldVal) {
        if ("id".equals(__fieldName)) {
            this.id = (Integer) __fieldVal;
            return true;
        }
        else    if ("widget_name".equals(__fieldName)) {
            this.widget_name = (String) __fieldVal;
            return true;
        }
        else    if ("price".equals(__fieldName)) {
            this.price = (java.math.BigDecimal) __fieldVal;
            return true;
        }
        else    if ("design_date".equals(__fieldName)) {
            this.design_date = (java.sql.Date) __fieldVal;
            return true;
        }
        else    if ("version".equals(__fieldName)) {
            this.version = (Integer) __fieldVal;
            return true;
        }
        else    if ("design_comment".equals(__fieldName)) {
            this.design_comment = (String) __fieldVal;
            return true;
        }
        else {
            return false;    }
    }
}
