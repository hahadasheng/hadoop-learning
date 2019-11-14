package com.hahadasheng.bigdata.hadooplearning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * @author Liucheng
 * @since 2019-11-06
 */
public class HDFSAppTest {

    private FileSystem fileSystem;

    @Before
    public void init() throws Exception{
        Configuration configuration = new Configuration();

        // 设置副本系数
        configuration.set("dfs.replication", "1");

        this.fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");
        System.out.println("~~~~~~~~~~~~~~test up ~~~~~~~~~~~~~~~~~~~~~~");
    }

    @After
    public void release() {
        System.out.println("\n~~~~~~~~~~~~~~test down ~~~~~~~~~~~~~~~~~~~~~~");

    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() throws IOException {
        // 创建文件夹
        Path path = new Path("/hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);
    }

    /**
     * 创建文件，并写入内容
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/b.txt"));

        out.writeUTF("hello pk");
        out.flush();
        out.close();
    }

    /**
     * 查看HDFS文件的内容: 文件内部的数据
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/b.txt"));
        // 将内容显示到控制台
        IOUtils.copyBytes(in, System.out, 1024);
    }

    /**
     * 副本系数
     * 1. 通过命令行上传的文件，副本系数为
     *    /$HADOOP_HOME/etc/hadoop/hdfs-site.xml
     *    中的dfs.replication配置为准。
     * 2. 通过Java客户端，默认是工具默认的系数
     *    org.apache.hadoop:hadoop-hdfs依赖下面的
     *    hdfs-default.xml中dfs.replication配置
     * 3. 如果需要定制，可以在Configuration对象中进行配置
     */
    @Test
    public void replicationTest() throws Exception {
        // 见上面副本系数设置
        this.create();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldName = new Path("/b.txt");
        Path newName = new Path("/c.txt");
        boolean result = fileSystem.rename(oldName, newName);
        System.out.println(result);
    }

    /**
     * copyFromLocalFile
     * 拷贝本地文件到HDFS文件系统
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        String path = Thread.currentThread().getContextClassLoader().getResource("localfile.txt").getPath();

        Path localFilePath = new Path(path);
        Path remoteFilePath = new Path("/remotefile.txt");
        fileSystem.copyFromLocalFile(localFilePath, remoteFilePath);
    }

    /**
     * 拷贝文件到HDFS文件系统，带上进度条
     */
    @Test
    public void copyFileWithProcessBar() throws IOException {
        String path = Thread.currentThread().getContextClassLoader().getResource("mysql.rar").getPath();

        InputStream in = new BufferedInputStream(new FileInputStream(new File(path)));
        FSDataOutputStream out = fileSystem.create(new Path("/mysql.rar"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(">");
            }
        });

        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 下载文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        String fileNameLocal = "E:/ImprovementWorkingSpace/hadoop-learning/src/test/resources/";
        Path src = new Path("/count.txt");
        Path dst = new Path(fileNameLocal);
        // 注意，Windows环境的得使用本地的文件系统！如下！
        fileSystem.copyToLocalFile(false, src, dst, true);
    }


    /**
     * 列出文件夹下面的内容
     */
    @Test
    public void listFile() throws Exception {
        FileStatus[] files = fileSystem.listStatus(new Path("/"));

        for (FileStatus file : files) {
            StringBuilder sb = new StringBuilder("~~~~~~~~~\n");
            sb.append("path:\t").append(file.getPath())
               .append("\nlength:\t").append(file.getLen())
               .append("\nisdir:\t").append(file.isDirectory())
               .append("\nblock_replication:\t").append(file.getReplication())
               .append("\nblocksize:\t").append(file.getBlockSize())
               .append("\nmodification_time:\t").append(file.getModificationTime())
               .append("\npermission:\t").append(file.getPermission())
               .append("\nowner:\t").append(file.getOwner())
               .append("\ngroup:\t").append(file.getGroup())
               .append("\nsymlink:\t").append(file.isSymlink())
               .append("~~~~~~~~~\n");

            System.out.println(sb.toString());
        }
    }

    /**
     * 递归列出文件:注意：只是文件，没有文件夹
     */
    @Test
    public void listFileRecursive() throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            StringBuilder sb = new StringBuilder("~~~~~~~~~\n");
            sb.append("path:\t").append(file.getPath())
                    .append("\nlength:\t").append(file.getLen())
                    .append("\nisdir:\t").append(file.isDirectory())
                    .append("\nblock_replication:\t").append(file.getReplication())
                    .append("\nblocksize:\t").append(file.getBlockSize())
                    .append("\nmodification_time:\t").append(file.getModificationTime())
                    .append("\npermission:\t").append(file.getPermission())
                    .append("\nowner:\t").append(file.getOwner())
                    .append("\ngroup:\t").append(file.getGroup())
                    .append("\nsymlink:\t").append(file.isSymlink())
                    .append("~~~~~~~~~\n");

            System.out.println(sb.toString());
        }
    }

    /**
     * 查看文件块信息: 文件分成几个块，副本等
     */
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/c.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks) {
            for (String name : block.getNames()) {
                System.out.print(name + " : " + block.getLength() + " : ");
                for (String host : block.getHosts()) {
                    System.out.print(host + "、");
                }
                System.out.println();
            }
        }
    }

    /**
     * 删除文件：选择递归或者非递归删除
     */
    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/user"), true);
        System.out.println(result);
    }

}
