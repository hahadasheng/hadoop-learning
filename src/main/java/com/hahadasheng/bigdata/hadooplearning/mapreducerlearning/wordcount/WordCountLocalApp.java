package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 本地任务测试：
 * 【强烈推荐】在本地测试，方便调试代码，代码没得问题后再打包丢到服务器进行调优！
 * @author Liucheng
 * @since 2019-11-14
 */
public class WordCountLocalApp {

    public static void main(String[] args) throws Exception {

        // 系统配置
        Configuration configuration = new Configuration();

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountLocalApp.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应参数：Mapper输出的key和value类型(泛型中后面两个的类型)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String pathIn = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\localtest\\wc.txt";
        String pathOut = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\localtest\\count";
        /*
        递归删除pathOut下的文件文件夹，然后再将pathOut空文件夹删除
        代码略
        */

        // 设置Job对应的参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path(pathIn));
        FileOutputFormat.setOutputPath(job,  new Path(pathOut));

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
