package com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.impl;

import com.hahadasheng.bigdata.hadooplearning.hdfslearning.comoon.Context;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.IWCMapper;


/**
 * 单词统计
 * @author Liucheng
 * @since 2019-11-09
 */
public class WordCountMapper implements IWCMapper {
    public WordCountMapper() {

    }

    @Override
    public void countAndCatch(String line, Context context) {

        String[] wordArr = line.split("[^a-zA-Z0-9]+");

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
