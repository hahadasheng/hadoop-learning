package com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper;

import com.hahadasheng.bigdata.hadooplearning.hdfslearning.comoon.Context;

/**
 * 统计单词的Mapper
 * @author Liucheng
 * @date 2019/11/9 13:37
 */
public interface IWCMapper {

    /**
     * @param line  读取到到每一行数据
     * @param context  上下文/缓存
     */
    public void countAndCatch(String line, Context context);

}
