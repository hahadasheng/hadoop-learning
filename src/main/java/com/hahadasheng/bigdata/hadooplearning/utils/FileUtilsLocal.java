package com.hahadasheng.bigdata.hadooplearning.utils;

import java.io.File;

/**
 * 本地文件工具类
 * @author Liucheng
 * @since 2019-11-16
 */
public class FileUtilsLocal {

    /**
     * 递归删除文件夹或者文件
     */
    public static boolean removeFileRecursion(File file) {

        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File everyFile : files) {
                removeFileRecursion(everyFile);
            }
        }

        return file.delete();
    }

    /**
     * 重载方法
     */
    public static boolean removeFileRecursion(String path) {
        File file = new File(path);
        if (file.exists()) {
            return removeFileRecursion(file);
        }

        return true;
    }
}
