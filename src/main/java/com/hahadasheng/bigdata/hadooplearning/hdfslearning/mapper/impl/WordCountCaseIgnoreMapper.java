package com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.impl;

import com.hahadasheng.bigdata.hadooplearning.hdfslearning.comoon.Context;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.IWCMapper;

/**
 * 单词统计，忽略大小写
 * @author Liucheng
 * @since 2019-11-09
 */
public class WordCountCaseIgnoreMapper implements IWCMapper {

    public WordCountCaseIgnoreMapper() {

    }

    @Override
    public void countAndCatch(String line, Context context) {
        String[] wordArr = line.toLowerCase().split("[^a-zA-Z0-9]+");

        for (String word : wordArr) {
            Object value = context.get(word);
            if(value == null) {
                // 第一次存单词
                context.write(word, 1);
            } else {
                context.write(word, (Integer)value + 1);
            }
        }
    }
}
