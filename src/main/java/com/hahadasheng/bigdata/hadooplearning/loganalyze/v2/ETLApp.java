package com.hahadasheng.bigdata.hadooplearning.loganalyze.v2;

import com.hahadasheng.bigdata.hadooplearning.utils.GetPageId;
import com.hahadasheng.bigdata.hadooplearning.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 将日志信息进行预处理
 * @author Liucheng
 * @since 2019-11-23
 */
public class ETLApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(ETLApp.class);

        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
/*

        String input = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\trackinfo_20130721.log";
        String output = "E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\logdata\\ETLData";
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


    static class ETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();
            Map<String, String> logInfo = LogParser.parse(log);

            String ip = logInfo.get("ip");
            String url = logInfo.get("url");
            String sessionId = logInfo.get("sessionId");
            String time = logInfo.get("time");
            String country = logInfo.get("country") == null ? "-" : logInfo.get("country");
            String province = logInfo.get("province")== null ? "-" : logInfo.get("province");
            String city = logInfo.get("city")== null ? "-" : logInfo.get("city");
            String pageId = StringUtils.isBlank(GetPageId.getPageId(url)) ? "-" : GetPageId.getPageId(url);

            StringBuilder sb = new StringBuilder(ip);
            sb.append("\t")
                    .append(url).append("\t")
                    .append(sessionId).append("\t")
                    .append(time).append("\t")
                    .append(country).append("\t")
                    .append(province).append("\t")
                    .append(city).append("\t")
                    .append(pageId);

/*            if (StringUtils.isNotBlank(pageId) && !pageId.equals("-")) {
                System.out.println("----- " + pageId);
            }*/

            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }
}
