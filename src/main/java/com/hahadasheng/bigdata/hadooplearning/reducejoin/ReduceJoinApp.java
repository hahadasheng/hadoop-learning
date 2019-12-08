package com.hahadasheng.bigdata.hadooplearning.reducejoin;

import lombok.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 前提：数据被ETL处理过，能被Hive直接使用
 * emp.txt
 *      empno   ename   sal
 * dept.txt
 *      deptno  dname
 *
 * emp.txt 与 dept.txt是多对一的关系
 * @author Liucheng
 * @since 2019-12-06
 */
public class ReduceJoinApp {

    public static void main(String[] args) throws Exception {

        // 配置类；默认为本地文件系统
        Configuration configuration = new Configuration();

        // Job工作类
        Job job = Job.getInstance(configuration);

        // 设置住主类
        job.setJarByClass(ReduceJoinApp.class);

        // 配置Reducer Task任务个数
        // job.setNumReduceTasks(3);

        // 配置Mapper与Reducer
        job.setMapperClass(ReduceJoinMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        // 告知Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Entity.class);

        // 告知Reducer的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 本地需要处理的文件
        Path emp = new Path("E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\join\\emp.txt");
        Path dept = new Path("E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\join\\dept.txt");

        Path outputPath = new Path("E:\\ImprovementWorkingSpace\\hadoop-learning\\src\\main\\resources\\join\\reduce-join");

        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(outputPath, true);

        MultipleInputs.addInputPath(job, emp, TextInputFormat.class);
        MultipleInputs.addInputPath(job, dept, TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

    }
}

class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Entity> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取文件名
        String filename = getFileName(context);

        String[] datas = value.toString().split("\t");

        if (filename.contains("emp.txt")) {
            if (datas.length < 8) {
                return;
            }

            // 员工表处理逻辑
            String deptno = datas[7];
            // 第一个属性flag表示标识来源
            Entity emp = new Entity("emp", datas[0], datas[1], datas[5], deptno);
            context.write(new Text(deptno), emp);
        } else {
            // 部门表处理逻辑
            if (datas.length < 3) {
                return;
            }
            String deptno = datas[0];
            Entity dept = new Entity("dept", datas[1]);
            context.write(new Text(deptno), dept);
        }
    }

    /**
     * 获取文件名的方法
     */

    public String getFileName(Context context) {

        // 获取记录对应的文件信息
        InputSplit inputSplit = context.getInputSplit();
        Class<? extends InputSplit> splitClass = inputSplit.getClass();
        FileSplit fileSplit = null;
        if (splitClass.equals(FileSplit.class)) {
            fileSplit = (FileSplit) inputSplit;
        } else if (splitClass.getName().equals("org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
            try {
                Method getInputSplitMethod = splitClass.getDeclaredMethod("getInputSplit");
                getInputSplitMethod.setAccessible(true);
                fileSplit = (FileSplit) getInputSplitMethod.invoke(inputSplit);
            } catch (Exception e) {
                System.out.println(e);
                throw new RuntimeException(e);
            }
        }

        // 获取文件名
        String fileName = fileSplit.getPath().getName();
        // 获取文件所在的路径名
        String filePath = fileSplit.getPath().getParent().toUri().getPath();

        return fileName;
    }
}


class ReduceJoinReducer extends Reducer<Text, Entity, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Entity> values, Context context) throws IOException, InterruptedException {

        List<Entity> emps = new ArrayList<>();
        Entity[] dept = new Entity[1];

        values.forEach(entity -> {
            if ("emp".equals(entity.getFlag())) {
                emps.add(new Entity(entity));
                System.out.println("刚取出来时" + entity);
            } else if ("dept".equals(entity.getFlag())) {
                dept[0] = new Entity(entity);
                System.out.println("刚取出来时" + entity);
            }
        });

        emps.forEach(entity -> {
            StringBuilder sb = new StringBuilder();
            sb.append(entity.getEmpnno()).append("\t")
                    .append(entity.getEname()).append("\t")
                    .append(entity.getSal()).append("\t")
                    .append(entity.getDeptno()).append("\t")
                    .append(dept[0].getDname());

            try {
                context.write(new Text(sb.toString()), NullWritable.get());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
class Entity implements Writable {

    private String flag;
    private String empnno;
    private String ename;
    private String sal;
    private String deptno;
    private String dname;

    public Entity(String flag, String empnno, String ename, String sal, String deptno) {
        this.flag = flag;
        this.empnno = empnno;
        this.ename = ename;
        this.sal = sal;
        this.deptno = deptno;
    }

    public Entity(String flag, String dname) {
        this.flag = flag;
        this.dname = dname;
    }

    public Entity(Entity newEntity) {
        this.flag = newEntity.getFlag();
        this.empnno = newEntity.getEmpnno();
        this.ename = newEntity.getEname();
        this.sal = newEntity.getSal();
        this.deptno = newEntity.getDeptno();
        this.dname = newEntity.getDname();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(flag);
        out.writeUTF(empnno != null ? empnno : "");
        out.writeUTF(ename != null ? ename : "");
        out.writeUTF(sal != null ? sal : "");
        out.writeUTF(deptno != null ? deptno : "");
        out.writeUTF(dname != null ? dname : "");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flag = in.readUTF();
        this.empnno = in.readUTF();
        this.ename = in.readUTF();
        this.sal = in.readUTF();
        this.deptno = in.readUTF();
        this.dname = in.readUTF();
    }

    @Override
    public String toString() {
        return this.hashCode() + "--Entity{" +
                "flag='" + flag + '\'' +
                ", empnno='" + empnno + '\'' +
                ", ename='" + ename + '\'' +
                ", sal='" + sal + '\'' +
                ", deptno='" + deptno + '\'' +
                ", dname='" + dname + '\'' +
                '}';
    }
}