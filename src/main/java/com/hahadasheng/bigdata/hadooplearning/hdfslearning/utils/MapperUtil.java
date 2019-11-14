package com.hahadasheng.bigdata.hadooplearning.hdfslearning.utils;

import com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.IWCMapper;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * 加载配置的Mapper
 * @author Liucheng
 * @since 2019-11-09
 */
public class MapperUtil {

    public static IWCMapper getMapper() {
        InputStream inputStream = MapperUtil.class.getClassLoader().getResourceAsStream("mapper.properties");
        Properties properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        String className = properties.getProperty("CLASSNAME");
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
            Constructor<?> constructor = clazz.getConstructor();
            return (IWCMapper)constructor.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("[" + MapperUtil.class.getName() + "] Mapper 加载失败");
        return null;
    }
}
