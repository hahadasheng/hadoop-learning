package com.hahadasheng.bigdata.hadooplearning.loganalyze.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author Liucheng
 * @since 2019-11-21
 */
public class PV2App {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(PV2App.class);

        job.setMapperClass(PV2Mapper.class);
        job.setReducerClass(PV2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
/*
        String input = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\ETLData\\*.log";
        String output = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\PV2Out";
*/
        String input = args[0];
        String output = args[1];

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }
    static class PV2Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Text oneKey = new Text("key");
        private LongWritable oneValue = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(oneKey, oneValue);
        }
    }

    static class PV2Reducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            final long[] count = {0};
            values.forEach(item -> count[0] += item.get());

            context.write(NullWritable.get(), new LongWritable(count[0]));
        }
    }
}


