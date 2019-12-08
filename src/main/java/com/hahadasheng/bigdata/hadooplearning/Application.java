package com.hahadasheng.bigdata.hadooplearning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author Liucheng
 * @since 2019-11-05
 */
public class Application {

    public static void main(String[] args) throws Exception{
        // 获取文件系统
        Configuration configuration = new Configuration();
        // 本地hosts配置了映射关系: 192.168.10.188 hadoop000
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");

        // 创建文件夹
        Path path = new Path("/hdfsapi/test");

        final boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }
}
