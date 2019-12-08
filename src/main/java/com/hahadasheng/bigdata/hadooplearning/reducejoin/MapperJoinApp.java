package com.hahadasheng.bigdata.hadooplearning.reducejoin;

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

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Liucheng
 * @since 2019-12-07
 */
public class MapperJoinApp {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        
        configuration.set("dfs.permissions", "false");
        
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MapperJoinApp.class);

        // 配置Reducer Task任务个数为0
        job.setNumReduceTasks(0);

        job.setMapperClass(MapperJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 小文件 dept.txt
        //URI dept = new URI(args[0]);
        URI dept = new URI("E:/ImprovementWorkingSpace/hadoop-learning/src/main/resources/join/dept.txt");
        // 将小文件加到分布式缓存中
        job.addCacheFile(dept);

        // 大文件 emp.txt
        //Path emp = new Path(args[1]);
        Path emp = new Path("E:/ImprovementWorkingSpace/hadoop-learning/src/main/resources/join/emp.txt");
        // 写入大文件
        FileInputFormat.setInputPaths(job, emp);

        // Path outputPath = new Path(args[2]);
        Path outputPath = new Path("E:/ImprovementWorkingSpace/hadoop-learning/src/main/resources/join/map-join");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(outputPath, true);
        // 文件输出
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }
}

class MapperJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> deptCatch = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        String filePath = context.getCacheFiles()[0].toString();
        BufferedReader br = new BufferedReader(new FileReader(filePath));

        String line;
        while ((line = br.readLine()) != null) {
            String[] datas = line.split("\t");
            // 部门表处理逻辑
            if (datas.length < 3) {
                return;
            }
            String deptno = datas[0];
            String dname = datas[1];
            deptCatch.put(deptno, dname);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = value.toString().split("\t");

        if (datas.length < 8) {
            return;
        }
        // 员工表处理逻辑
        String empnno = datas[0];
        String ename = datas[1];
        String sal = datas[5];
        String deptno = datas[7];

        StringBuilder sb = new StringBuilder();
        sb.append(empnno).append("\t")
                .append(empnno).append("\t")
                .append(ename).append("\t")
                .append(sal).append("\t")
                .append(deptno).append("\t")
                .append(deptCatch.get(deptno));

        context.write(new Text(sb.toString()), NullWritable.get());
    }
}