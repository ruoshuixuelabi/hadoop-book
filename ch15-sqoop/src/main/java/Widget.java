// ORM class for table 'widgets'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Oct 29 09:58:23 GMT 2014
// For connector: org.apache.sqoop.manager.MySQLManager
// 下面这个类对应关系数据库里的 widgets 表，
// 六个字段 id、widget_name、price、design_date、version、design_comment
// 正好对应表里的六列
//
// 值得看的地方在于：它要同时满足两边的要求——
// 数据库那边（SqoopRecord、DBWritable、ResultSet/PreparedStatement）
// 和 Hadoop 这边（Writable、DataInput/DataOutput）
// 所以每个字段都有成对的读写方法：
// 要么从 JDBC 结果集里读，要么从 Hadoop 的二进制流里反序列化
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
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
 * Sqoop 为 widgets 表生成的记录类
 *
 * <p>它是 Sqoop 自动生成的（文件头有警告，不要手工改），
 * 生成的规矩是：每个数据库列对应一个私有字段，
 * 再配上 getter、setter 和链式 setter（{@code with_xxx}）
 * <p>字段按名字而不是按位置取值，
 * 是因为通用（generic）模式下没有编译期生成的类可用
 */
public class Widget extends SqoopRecord  implements DBWritable, Writable {
  // 记录格式的版本号：Sqoop 用它判断新旧格式能否兼容
  private final int PROTOCOL_VERSION = 3;
  /**
   * 取记录格式的版本号
   *
   * @return 格式版本号
   */
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  // 读字段时暂存 JDBC 的结果集
  protected ResultSet __cur_result_set;
  // 主键
  private Integer id;
  /**
   * 取主键
   *
   * @return 主键
   */
  public Integer get_id() {
    return id;
  }
  /**
   * 设置主键
   *
   * @param id 主键
   */
  public void set_id(Integer id) {
    this.id = id;
  }
  /**
   * 设置主键并返回自己
   *
   * <p>用来把赋值写成一行：{@code new Widget().with_id(1).with_widget_name("x")}
   *
   * @param id 主键
   * @return 这条记录
   */
  public Widget with_id(Integer id) {
    this.id = id;
    return this;
  }
  // 名称
  private String widget_name;
  /**
   * 取名称
   *
   * @return 名称
   */
  public String get_widget_name() {
    return widget_name;
  }
  /**
   * 设置名称
   *
   * @param widget_name 名称
   */
  public void set_widget_name(String widget_name) {
    this.widget_name = widget_name;
  }
  /**
   * 设置名称并返回自己
   *
   * @param widget_name 名称
   * @return 这条记录
   */
  public Widget with_widget_name(String widget_name) {
    this.widget_name = widget_name;
    return this;
  }
  // 价格
  private java.math.BigDecimal price;
  /**
   * 取价格
   *
   * @return 价格
   */
  public java.math.BigDecimal get_price() {
    return price;
  }
  /**
   * 设置价格
   *
   * @param price 价格
   */
  public void set_price(java.math.BigDecimal price) {
    this.price = price;
  }
  /**
   * 设置价格并返回自己
   *
   * @param price 价格
   * @return 这条记录
   */
  public Widget with_price(java.math.BigDecimal price) {
    this.price = price;
    return this;
  }
  // 设计日期
  private java.sql.Date design_date;
  /**
   * 取设计日期
   *
   * @return 设计日期
   */
  public java.sql.Date get_design_date() {
    return design_date;
  }
  /**
   * 设置设计日期
   *
   * @param design_date 设计日期
   */
  public void set_design_date(java.sql.Date design_date) {
    this.design_date = design_date;
  }
  /**
   * 设置设计日期并返回自己
   *
   * @param design_date 设计日期
   * @return 这条记录
   */
  public Widget with_design_date(java.sql.Date design_date) {
    this.design_date = design_date;
    return this;
  }
  // 版本号
  private Integer version;
  /**
   * 取版本号
   *
   * @return 版本号
   */
  public Integer get_version() {
    return version;
  }
  /**
   * 设置版本号
   *
   * @param version 版本号
   */
  public void set_version(Integer version) {
    this.version = version;
  }
  /**
   * 设置版本号并返回自己
   *
   * @param version 版本号
   * @return 这条记录
   */
  public Widget with_version(Integer version) {
    this.version = version;
    return this;
  }
  // 设计说明
  private String design_comment;
  /**
   * 取设计说明
   *
   * @return 设计说明
   */
  public String get_design_comment() {
    return design_comment;
  }
  /**
   * 设置设计说明
   *
   * @param design_comment 设计说明
   */
  public void set_design_comment(String design_comment) {
    this.design_comment = design_comment;
  }
  /**
   * 设置设计说明并返回自己
   *
   * @param design_comment 设计说明
   * @return 这条记录
   */
  public Widget with_design_comment(String design_comment) {
    this.design_comment = design_comment;
    return this;
  }
  /**
   * 比较两条记录是否相等
   *
   * <p>逐个字段比：两边都为空算相等，
   * 只要有一个字段不等就整体不等
   *
   * @param o 要比较的对象
   * @return 相等时为 true
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
    // 逐个字段比：主键先比
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.widget_name == null ? that.widget_name == null : this.widget_name.equals(that.widget_name));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    equal = equal && (this.design_date == null ? that.design_date == null : this.design_date.equals(that.design_date));
    equal = equal && (this.version == null ? that.version == null : this.version.equals(that.version));
    equal = equal && (this.design_comment == null ? that.design_comment == null : this.design_comment.equals(that.design_comment));
    return equal;
  }
  /**
   * 比较两条记录是否相等（SqlMap 的备用版本）
   *
   * <p>和 {@link #equals(Object)} 做的事一样，
   * 是 Sqoop 生成代码时留下的另一个版本
   *
   * @param o 要比较的对象
   * @return 相等时为 true
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
   * 从 JDBC 结果集里把六个字段读出来
   *
   * <p>列号是从 1 开始的，顺序要和建表语句里的列顺序一致
   *
   * @param __dbResults JDBC 结果集
   * @throws SQLException 读取失败
   */
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.widget_name = JdbcWritableBridge.readString(2, __dbResults);
    this.price = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.design_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.version = JdbcWritableBridge.readInteger(5, __dbResults);
    this.design_comment = JdbcWritableBridge.readString(6, __dbResults);
  }
  /**
   * 从 JDBC 结果集里把六个字段读出来（SqlMap 的备用版本）
   *
   * <p>和 {@link #readFields(ResultSet)} 做的事一样，
   * 区别只是不保存结果集本身
   *
   * @param __dbResults JDBC 结果集
   * @throws SQLException 读取失败
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
   * 加载大对象字段
   *
   * <p>widgets 表里没有 BLOB/CLOB 之类的字段，
   * 所以这里是空的
   *
   * @param __loader 大对象加载器
   * @throws SQLException 加载失败
   * @throws IOException 读取失败
   * @throws InterruptedException 任务被中断
   */
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  /**
   * 加载大对象字段（SqlMap 的备用版本）
   *
   * @param __loader 大对象加载器
   * @throws SQLException 加载失败
   * @throws IOException 读取失败
   * @throws InterruptedException 任务被中断
   */
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  /**
   * 把六个字段写回数据库
   *
   * @param __dbStmt 预编译语句
   * @throws SQLException 写入失败
   */
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  /**
   * 把六个字段写回数据库，列号从 {@code __off + 1} 开始
   *
   * @param __dbStmt 预编译语句
   * @param __off 列号的偏移量
   * @return 写出的字段个数
   * @throws SQLException 写入失败
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
   * 把六个字段写回数据库（SqlMap 的备用版本）
   *
   * @param __dbStmt 预编译语句
   * @param __off 列号的偏移量
   * @throws SQLException 写入失败
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
   * 从 Hadoop 的二进制流里把六个字段读出来
   *
   * <p>这是 Hadoop 侧的读法：
   * 每个字段先写一个布尔值表示「是不是空的」，
   * 不是空才写具体值，于是空值和空串可以区分开
   *
   * @param __dataIn 数据输入流
   * @throws IOException 读取失败
   */
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  /**
   * 从 Hadoop 的二进制流里把六个字段读出来（SqlMap 的备用版本）
   *
   * @param __dataIn 数据输入流
   * @throws IOException 读取失败
   */
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.widget_name = null;
    } else {
    this.widget_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.price = null;
    } else {
    this.price = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.design_date = null;
    } else {
    this.design_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.version = null;
    } else {
    this.version = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.design_comment = null;
    } else {
    this.design_comment = Text.readString(__dataIn);
    }
  }
  /**
   * 把六个字段写进 Hadoop 的二进制流
   *
   * <p>和 {@link #readFields(DataInput)} 一一对应：
   * 先写「是不是空的」，不是空才写具体值
   *
   * @param __dataOut 数据输出流
   * @throws IOException 写出失败
   */
  public void write(DataOutput __dataOut) throws IOException {
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
  /**
   * 把六个字段写进 Hadoop 的二进制流（SqlMap 的备用版本）
   *
   * @param __dataOut 数据输出流
   * @throws IOException 写出失败
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
  // 输出用的分隔符：字段之间用逗号，记录之间用换行
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  /**
   * 按默认分隔符把记录转成文本
   *
   * @return 文本形式
   */
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  /**
   * 按给定的分隔符把记录转成文本
   *
   * @param delimiters 分隔符
   * @return 文本形式
   */
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  /**
   * 按默认分隔符把记录转成文本，可选择要不要带记录结束符
   *
   * @param useRecordDelim 是否在末尾加上记录结束符
   * @return 文本形式
   */
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  /**
   * 按给定的分隔符把记录转成文本
   *
   * @param delimiters 分隔符
   * @param useRecordDelim 是否在末尾加上记录结束符
   * @return 文本形式
   */
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
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
   * 按给定的分隔符把记录写进已有的 StringBuilder（SqlMap 的备用版本）
   *
   * @param delimiters 分隔符
   * @param __sb 接收结果的 StringBuilder
   * @param fieldDelim 字段分隔符
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
  // 解析时用的分隔符：和输出保持一致
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  // 延迟创建：第一次 parse 时才建，之后复用
  private RecordParser __parser;
  /**
   * 把一行文本解析成一条记录
   *
   * <p>先由 {@link RecordParser} 按分隔符把行拆成字段，
   * 再由 {@code __loadFromFields} 逐个赋值
   *
   * @param __record 一行文本
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把一行文本解析成一条记录
   *
   * @param __record 一行文本（字符序列形式）
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把一行文本解析成一条记录
   *
   * @param __record 一行文本（字节数组形式）
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把一行文本解析成一条记录
   *
   * @param __record 一行文本（字符数组形式）
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把一行文本解析成一条记录
   *
   * @param __record 一行文本（ByteBuffer 形式）
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把一行文本解析成一条记录
   *
   * @param __record 一行文本（CharBuffer 形式）
   * @throws RecordParser.ParseError 记录格式不对时抛出
   */
  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  /**
   * 把拆开的字段逐个填进对象
   *
   * <p>文本里的 {@code "null"} 表示这个字段是空的
   *
   * @param fields 拆开的字段
   */
  private void __loadFromFields(List<String> fields) {
    // 逐个取字段
    Iterator<String> __it = fields.listIterator();
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

    // 字段类型对不上时，把出错的字段一并报出来
    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  /**
   * 把拆开的字段逐个填进对象（SqlMap 的备用版本）
   *
   * @param __it 字段的迭代器
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

    // 字段类型对不上时，把出错的字段一并报出来
    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  /**
   * 复制一条记录
   *
   * <p>普通的浅拷贝还不够：
   * {@code design_date} 是可变对象，得单独复制一份
   *
   * @return 复制出来的新记录
   * @throws CloneNotSupportedException 不支持复制时抛出
   */
  public Object clone() throws CloneNotSupportedException {
    Widget o = (Widget) super.clone();
    o.design_date = (o.design_date != null) ? (java.sql.Date) o.design_date.clone() : null;
    return o;
  }

  /**
   * 复制一条记录（SqlMap 的备用版本）
   *
   * @param o 复制到哪条记录
   * @throws CloneNotSupportedException 不支持复制时抛出
   */
  public void clone0(Widget o) throws CloneNotSupportedException {
    o.design_date = (o.design_date != null) ? (java.sql.Date) o.design_date.clone() : null;
  }

  /**
   * 把所有字段按名字装进一个 Map
   *
   * <p>用 {@link TreeMap} 是为了让字段的顺序固定下来
   *
   * @return 字段名到字段值的映射
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
   * 把所有字段按名字装进一个已有的 Map（SqlMap 的备用版本）
   *
   * @param __sqoop$field_map 接收结果的 Map
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
   * 按字段名设置字段值
   *
   * @param __fieldName 字段名
   * @param __fieldVal 字段值
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
   * 按字段名设置字段值（SqlMap 的备用版本）
   *
   * @param __fieldName 字段名
   * @param __fieldVal 字段值
   * @return 设置成功时为 true；字段名不认识时为 false
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
