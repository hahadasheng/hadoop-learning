package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 1. 同样注意包名
 * 2. 实现的泛型
 *    1. KEYIN: 对应Mapper输出的key
 *    2. VALUEIN: 对应Mapper输出的value
 *    3. KEYOUT: 处理结果key
 *    4. VALUEOUT: 处理结果value
 *
 * @author Liucheng
 * @since 2019-11-12
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    /** 【模板设计模式】
     *  map的输出到reduce端，是按照相同的key分发到一个reduce上去执行
     *  此时的数据关系类似以下的举例                      key    values
     *    (hello,1)(hello,1)(hello,1) ==> reduce1: (hello, <1,1,1>)
     *    (world,1)(world,1)(world,1) ==> reduce2: (world, <1,1,1>)
     *    (welcome,1)                 ==> reduce3: (welcome, <1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();

        // <1,1,1>
        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
