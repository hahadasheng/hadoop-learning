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
 * 提交作业到Yarn环境
 */
public class AccessAppYarnLocal {

    public static void main(String[] args) throws Exception {

        // 配置类；默认为本地文件系统
        Configuration configuration = new Configuration();

        // Job工作类
        Job job = Job.getInstance(configuration);

        // 设置住主类
        job.setJarByClass(AccessAppYarnLocal.class);

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

        // hdfs文件系统的读写操作
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // 确保输出目录不存在！如果存在就会删除【此操作比较危险】
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8082"), configuration, "hadoop");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
}
