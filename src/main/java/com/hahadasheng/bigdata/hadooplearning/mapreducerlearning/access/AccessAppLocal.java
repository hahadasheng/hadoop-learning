package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import com.hahadasheng.bigdata.hadooplearning.utils.FileUtilsLocal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 本地文件测试：后期丢到服务器上进行测试
 * @author Liucheng
 * @since 2019-11-16
 */
public class AccessAppLocal {

    public static void main(String[] args) throws Exception {

        // 配置类；默认为本地文件系统
        Configuration configuration = new Configuration();

        // Job工作类
        Job job = Job.getInstance(configuration);

        // 设置住主类
        job.setJarByClass(AccessAppLocal.class);

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
        String inputPath = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\source-data";
        String outputPath = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\source-data\\count";
        FileUtilsLocal.removeFileRecursion(outputPath);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
