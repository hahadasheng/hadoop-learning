package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author Liucheng
 * @since 2019-11-17
 */
public class AccessAppRemote {

    public static void main(String[] args) throws Exception {
        // 设置系统“环境变量”；用于hadoop程序读取配置
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 配置类; 配置为远程hdfs文件系统
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop000:8020");

        // Job工作类
        Job job = Job.getInstance(configuration);

        // 设置主类
        job.setJarByClass(AccessAppRemote.class);

        // combiner操作
        job.setCombinerClass(AccessCombiner.class);

        // 配置Reducer Task任务个数
        job.setNumReduceTasks(3);

        // 配置自定义Partitioner
        job.setPartitionerClass(AccessPartitioner.class);

        // 配置Mapper与Reducer
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // 告知Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        // 告知Reducer的输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        // 本地需要处理的文件
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");

        Path inputPath = new Path("/access");
        Path outputPath = new Path("/access/count");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        // 设置Job对应的参数：作业输入和输出的路径
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
    }
}
