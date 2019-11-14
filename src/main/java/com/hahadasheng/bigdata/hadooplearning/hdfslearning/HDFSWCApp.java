package com.hahadasheng.bigdata.hadooplearning.hdfslearning;

import com.hahadasheng.bigdata.hadooplearning.hdfslearning.comoon.Context;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.configuration.FileSystemInfo;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.configuration.WordWCPathInfo;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.mapper.IWCMapper;
import com.hahadasheng.bigdata.hadooplearning.hdfslearning.utils.MapperUtil;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;

/**
 * @author Liucheng
 * @since 2019-11-09
 */
public class HDFSWCApp {

    public static void main(String[] args) throws Exception {
        // 1. 获取文件系统句柄
        FileSystem fileSystem = FileSystemInfo.getFileSystem();

        // 2. 获取需要统计的文件
        Path remotePath = new Path(WordWCPathInfo.getSourcePath());

        // 3. 获取上下文
        Context context = Context.getInstance();
        // 4. 获取统计工具
        IWCMapper mapper = MapperUtil.getMapper();

        // 3. 获取文件迭代器
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(remotePath, false);

        // 4. 遍历文件迭代器获取流,并存入上下文
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fileSystem.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line;
            while ((line = reader.readLine()) != null) {
                mapper.countAndCatch(line, context);
            }

            // 关闭输入流
            reader.close();
            in.close();
        }

        // 5. todo 获取输入流
        Path outPath = new Path(WordWCPathInfo.getDstPath());
        FSDataOutputStream out = fileSystem.create(outPath);

        // 6. 遍历缓存，将统计写入输出流
        Set<Map.Entry<Object, Object>> entries = context.getCacheMap().entrySet();
        entries.forEach(entry -> {
            try {
                String lineRes = entry.getKey() + " \t " + entry.getValue()+ "\n";
                System.out.print(lineRes);
                out.write(lineRes.getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // 7.释放资源
        context.clear();
        out.close();
        fileSystem.close();
    }
}
