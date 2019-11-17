package com.hahadasheng.bigdata.hadooplearning.mapreducerlearning.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Liucheng
 * @since 2019-11-15
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lineList = value.toString().split("[\t]+");
        int length = lineList.length;

        if (length <= 7) {
            return;
        }

        String phone = lineList[1];
        if (phone == null) {
            return;
        }

        try {
            Long.valueOf(phone);
        } catch (Exception e) {
            return ;
        }

        Long up;
        Long down;

        try {
            up = Long.valueOf(lineList[length - 3]);
        } catch (Exception e) {
            up = 0L;
        }

        try {
            down = Long.valueOf(lineList[length - 2]);
        } catch (Exception e) {
            down = 0L;
        }

        Access access = new Access();

        access.setPhone(phone);
        access.setUp(up);
        access.setDown(down);

        context.write(new Text(phone), access);
    }
}
