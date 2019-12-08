package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1. 注意实现Mapper的包包名
 * 2. 抽象类中的泛型
 *    1. KEYIN: Map任务读数据的key类型，offset，是每行数据起始位置的偏移量，一般为Long
 *    2. VALUEIN: Map任务读数据的value类型，其实就是一行行的字符串，例如String
 *    3. KEYOUT: map方法自定义实现输出的key的类型，例如String
 *    4. VALUEOUT: map方法自定义实现输出的value的类型，例如Integer
 *
 * 3. Hadoop分布式文件系统，数据交互经过网络，必然涉及到【序列化】【反序列化】，
 *    它有自己的数据类型支持处理这些要求(实现对应的接口)
 *
 * 4. 案例：词频统计：相同单词的次数(word, 1) 注意，这里不需要计算！！只是中间转换！！
 * 5. Long,String,Integer是Java里面的数据类型，Hadoop中可以对应使用
 *    LongWritable,Text,IntWritable
 *
 * @author Liucheng
 * @since 2019-11-12
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /** 【模板设计模式】
     * 重写这个方法，进行中间转换并放入【上下文环境】
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        final String[] words = value.toString().split("[^a-zA-Z0-9]+");

        for (String word : words) {
            // (hello, 1) (word, 1)
            // 不区分大小写！
            // 注意，这里只是转换，不计算；
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
