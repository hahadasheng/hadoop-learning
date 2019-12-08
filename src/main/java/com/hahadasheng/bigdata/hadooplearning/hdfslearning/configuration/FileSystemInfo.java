package com.hahadasheng.bigdata.hadooplearning.hdfslearning.configuration;

import jdk.internal.util.xml.impl.ReaderUTF8;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;
import java.util.Properties;

/**
 * 初始化文件系统
 * @author Liucheng
 * @date 2019/11/9 13:38
 */
@Getter
public class FileSystemInfo {

    private static FileSystem fileSystem;

    static {
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = FileSystemInfo.class.getClassLoader().getResourceAsStream("file-system-info.properties");
            Reader reader = new ReaderUTF8(inputStream);
            properties.load(reader);

            String user = properties.getProperty("FILESYSTEM.USER");
            URI uri = new URI(properties.getProperty("FILESYSTEM.URI"));
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication", properties.getProperty("DFS.REPLICATION"));

            fileSystem = FileSystem.get(uri, configuration, user);

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("["+ FileSystemInfo.class.getName() + "] 文件系统初始化配置错误！");
            System.exit(1);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static FileSystem getFileSystem() {
        return fileSystem;
    }

<<<<<<< HEAD
=======

>>>>>>> a9d37a311ae07a749a2085cf4e67935b56c4854a
}
