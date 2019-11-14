package com.hahadasheng.bigdata.hadooplearning.hdfslearning.configuration;

import jdk.internal.util.xml.impl.ReaderUTF8;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**
 * 单词统计信息配置
 * @author Liucheng
 * @since 2019-11-09
 */
public class WordWCPathInfo {

    private static String sourcePath;
    private static String dstPath;

    static {
        Properties properties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = FileSystemInfo.class.getClassLoader().getResourceAsStream("words-wc-info.properties");
            Reader reader = new ReaderUTF8(inputStream);
            properties.load(reader);

            sourcePath = properties.getProperty("FILE.SOURCE.PATH");
            dstPath = properties.getProperty("FILE.DST.PATH");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("["+ FileSystemInfo.class.getName() + "] 任务信息初始化配置读取错误！");
            System.exit(1);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getSourcePath() {
        return sourcePath;
    }

    public static String getDstPath() {
        return dstPath;
    }
}
