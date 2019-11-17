package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Liucheng
 * @since 2019-11-16
 */
public class AccessCombiner extends Reducer<Text, Access, Text, Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        Access access = new Access();

        Iterator<Access> iterator = values.iterator();
        while (iterator.hasNext()) {
            Access next = iterator.next();
            access.setUp(access.getUp() + next.getUp());
            access.setDown(access.getDown() + next.getDown());
        }

        access.setPhone(key.toString());
        access.setSum(access.getUp() + access.getDown());
        context.write(key, access);
    }
}
