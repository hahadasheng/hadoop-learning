package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ Combiner操作.png
 *
 * 1. Combiner肯定是用了反射的技术
 * 2. Combiner操作之前应该也执行了类似shuffle的操作，拿到转换成key以及对应的value迭代器，
 *    处理后交给上下文，然后再shuffle，转换成key以及对应的value迭代器丢给下一个Reducer处理
 *
 * @author Liucheng
 * @since 2019-11-14
 */
public class WordCountCombinerApp {

    public static void main(String[] args) throws Exception {

        // 设置系统“环境变量”; 用于hadoop程序读取配置
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 系统配置
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.10.188:8020");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置Job对应的参数：主类
        job.setJarByClass(WordCountCombinerApp.class);

        // 添加Combiner的设置@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2
        job.setCombinerClass(WordCountReducer.class);

        // 设置Job对应的参数：设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Job对应参数：Mapper输出的key和value类型(泛型中后面两个的类型)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Job对应的参数：Reduce输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 获取hdfs句柄：如果目录已经存在，则先删除，否则会报错！
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.10.188:8020"), configuration, "hadoop");
        Path outputPath = new Path("/wordcount/output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 设置Job对应的参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }
}
