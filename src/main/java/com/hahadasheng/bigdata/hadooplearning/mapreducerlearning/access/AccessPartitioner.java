package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 1. 泛型必须是Mapper的输出或者是Combiner的输出类型
 * 2. numPartitions默认配置为 1，可在job中配置
 *    配置多少个就会有多少个Reduce Task执行；
 *    每个Task负责输出到一个独立的分区文件。
 *
 * @author Liucheng
 * @since 2019-11-17
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        // 假设配置3个reduce Task; 输出文件也会对应3个
        String key = text.toString();

        if (key.startsWith("13")) {
            return 0;
        } else if (key.startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
