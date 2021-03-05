package org.puppy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/5 14:00
 */
public class TestHadoopFileSystem {
    private final static Logger LOGGER = LoggerFactory.getLogger(TestHadoopFileSystem.class);

    private final static String HDFS_NAME_NODE_URL = "hdfs://hadoop000:9000";

    private FileSystem fileSystem;

    private final static String ROOT = "/user/puppy";

    private Configuration configuration;

    @BeforeEach
    public void setUp() throws IOException, URISyntaxException, InterruptedException {
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI(HDFS_NAME_NODE_URL),
                configuration, "puppy");
    }

    @Test
    public void mkdirs() throws IOException {
        boolean result = fileSystem.mkdirs(new Path(ROOT));
        LOGGER.info("创建目录" + (result ? "成功" : "失败"));
    }

    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path(ROOT + "/hadoop-hello-world.text"));
        IOUtils.copyBytes(in, System.out, 1024, true);
    }

    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path(ROOT + "/hadoop-hello-world.text"));

        out.writeBytes("Hello world for hadoop.");

        out.flush();

        out.close();
    }

    @Test
    public void rename() throws Exception {

        boolean result = fileSystem.rename(new Path(ROOT + "/hadoop-hello-world-rp1.text"),
                new Path(ROOT + "/hadoop-hello-world-modify.text"));

        LOGGER.info("重名名" + (result ? "成功" : "失败"));
    }

    @Test
    public void config() throws IOException {

        LOGGER.info("副本数量:" + configuration.get("dfs.replication"));

        FSDataOutputStream out = fileSystem.create(new Path(ROOT + "/hadoop-hello-world-rp1.text"));

        out.writeBytes("Hello world for hadoop.");

        out.flush();

        out.close();

    }

    @Test
    public void copyFromLocal() throws Exception {
        fileSystem.copyFromLocalFile(new Path("D:\\opt\\app\\account\\logs\\all.log"),
                new Path(ROOT + "/all.log"));
        LOGGER.info("拷贝成功");
    }

    /**
     * 带进度条的拷贝
     */
    @Test
    public void copyBigFileFromLocal() throws Exception {

        FileInputStream in = new FileInputStream(new File("D:\\Downloads\\BaiduNetdiskDownload\\StarUMLSetup3.1.0.exe"));

        FSDataOutputStream out = fileSystem.create(new Path(ROOT + "/startup.gzip"), () -> {
            System.out.print(".");
        });

        IOUtils.copyBytes(in, out, 1024, true);

        LOGGER.info("拷贝大文件成功");
    }

    @Test
    public void copyToLocalFile() throws IOException {
        fileSystem.copyToLocalFile(new Path(ROOT + "/all.log"),
                new Path("D:\\"));
        LOGGER.info("下载成功");
    }

    @Test
    public void list() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(ROOT + "/"), true);
        LOGGER.info("下载成功");

        while (iterator.hasNext()) {
            LocatedFileStatus file = iterator.next();
            String isDir = file.isDirectory() ? "文件夹" : "文件";

            String permission = file.getPermission().toString();

            short replication = file.getReplication();

            String path = file.getPath().getName();

            LOGGER.info(isDir + " " + permission + " " + replication + " " + path);

        }
    }

    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus file = fileSystem.getFileStatus(new Path(ROOT));

        BlockLocation[] locations = fileSystem.getFileBlockLocations(file, 0, file.getLen());

        for (BlockLocation location : locations) {
            LOGGER.info(location + " " + location.getOffset());
        }
    }


    @Test
    public void delete() throws IOException {
        boolean result = fileSystem.delete(new Path(ROOT + "/startup.gzip"), false);
        LOGGER.info("删除" + (result ? "成功" : "失败"));
    }

    @AfterEach
    public void tearDown() throws IOException {
        fileSystem.close();
        fileSystem = null;
    }


}
