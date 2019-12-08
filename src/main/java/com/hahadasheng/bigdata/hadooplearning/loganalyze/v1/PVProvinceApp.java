package com.hahadasheng.bigdata.hadooplearning.loganalyze.v1;

import com.hahadasheng.bigdata.hadooplearning.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Liucheng
 * @since 2019-11-22
 */
public class PVProvinceApp {

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(PVProvinceApp.class);

        job.setMapperClass(PVProvinceMapper.class);
        job.setReducerClass(PVProvinceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setCombinerClass(PVProvinceReducer.class);

        String input = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\trackinfo_20130721.log";
        String output = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\PVProvinceOut";
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);

        // FileUtilsLocal.removeFileRecursion(output);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    static class PVProvinceMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable ONE;

        /**
         * 初始化调用方法,init操作
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            ONE = new LongWritable(1);
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Map<String, String> areaMsg = LogParser.parse(value.toString());
            String province = areaMsg.get("province");
            if (StringUtils.isBlank(province)) {
                province = "_";
            }
            context.write(new Text(province), ONE);
        }
    }

    static class PVProvinceReducer extends Reducer<Text, LongWritable,Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            AtomicLong count = new AtomicLong();
            values.forEach(longWritable -> count.addAndGet(longWritable.get()));

            context.write(key, new LongWritable(count.longValue()));
        }
    }

}

