package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import lombok.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** 【模板方法模式】
 * 自定义复杂数据类型->以支持Hadoop在网络传输的性质(实现相关接口方法即可)
 * 1) 按照Hadoop的规范，需要实现Writable接口
 * 2）按照Hadoop的规范，需要实现write和readFields这两个方法
 * 3）定义一个默认的构造方法
 *
 * @author Liucheng
 * @since 2019-11-15
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Access implements Writable {

    private String phone; // 手机号
    private long up;      // 上行流量
    private long down;    // 下行流量
    private long sum;     // 上行流量和下行流量之和

    /** 【写入与读取的操作是框架做的事】
     * 1. DataOutput 是在网络传输时的序列化对象
     * 2. 通过调用对应的接口将数据封装到DataOutput对象中
     * 3. 如果要写入null，得为 NullWritable.get()
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.phone); // 写字符串的方法
        out.writeLong(this.up);
        out.writeLong(this.down);
        out.writeLong(this.sum);
    }

    /**
     * 1. DataInput 是接受传输后的反序列化对象；
     * 2. 【读取的数据赋值必须与被写入的顺序一致！】参见上面write方法
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    /**
     * 文件输出可能会用到
     */
    @Override
    public String toString() {
        return  phone +
                "," + up +
                "," + down +
                "," + sum;
    }
}
