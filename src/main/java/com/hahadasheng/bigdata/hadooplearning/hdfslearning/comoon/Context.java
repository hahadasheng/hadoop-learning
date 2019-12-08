package com.hahadasheng.bigdata.hadooplearning.hdfslearning.comoon;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Liucheng
 * @since 2019-11-09
 */
public class Context {

    private static final Context context = new Context();

    public static Context getInstance() {
        return context;
    }

    /**
     * 缓存Map
     */
    final Map<Object, Object> cacheMap = new ConcurrentHashMap<>();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    /**
     * 写数据到缓存中去
     * @param key 单词
     * @param value 次数
     */
    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    /**
     * 从缓存中获取值
     * @param key 单词
     * @return  单词对应的词频
     */
    public Object get(Object key) {
        return cacheMap.get(key);
    }

    /**
     * 清空缓存
     */
    public void clear() {
        cacheMap.clear();
    }
}
