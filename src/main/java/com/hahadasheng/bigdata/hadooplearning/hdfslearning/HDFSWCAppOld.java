package com.hahadasheng.bigdata.hadooplearning.hdfslearning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用HDFS API完成wordcount统计
 *
 * 需求：统计HDFS上的文件的wc，然后将统计结果输出到HDFS
 *
 * 功能拆解：
 * 1）读取HDFS上的文件 ==> HDFS API
 * 2）业务处理(词频统计)：对文件中的每一行数据都要进行业务处理（按照分隔符分割） ==> Mapper
 * 3）将处理结果缓存起来   ==> Context
 * 4）将结果输出到HDFS ==> HDFS API
 * @author Liucheng
 * @since 2019-11-08
 */
@Deprecated
public class HDFSWCAppOld {

    public static void main(String[] args) throws Exception {

        // 1. 获取文件系统句柄
        Configuration configuration = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000"), configuration, "hadoop");

        // 2. todo 获取需要统计的文件
        final Path root = new Path("/");
        final Path remotePath = new Path(root, new Path("apache-hadoop.txt"));

        // 3. 获取文件迭代器
        final RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(remotePath, false);

        // 4. 遍历文件迭代器获取流,并存入缓存
        final Map<String, Integer> catche = new ConcurrentHashMap<>();
        while (iterator.hasNext()) {
            final LocatedFileStatus file = iterator.next();
            final FSDataInputStream in = fileSystem.open(file.getPath());
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line;
            while ((line = reader.readLine()) != null) {

                // todo 统计计算
                final String[] wordArr = line.split("[^a-zA-Z0-9]+");

                for (String word : wordArr) {
                    catche.merge(word, 1, (a, b) -> a + b);
                }
            }

            // 关闭输入流
            reader.close();
            in.close();
        }


        // 5. todo 获取输入流
        final Path outPath = new Path("/count.txt");
        final FSDataOutputStream out = fileSystem.create(outPath);

        // 6. 遍历缓存，将统计写入输出流
        final Set<Map.Entry<String, Integer>> entries = catche.entrySet();
        entries.forEach(entry -> {
            try {
                System.out.println(entry.getKey() + " \t " + entry.getValue());
                out.writeUTF(entry.getKey() + " \t " + entry.getValue() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // 7.释放资源
        catche.clear();
        out.close();
        fileSystem.close();
    }
}
