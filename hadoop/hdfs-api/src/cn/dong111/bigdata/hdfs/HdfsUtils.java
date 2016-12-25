package cn.dong111.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * java操作hdfs客户端程序
 * Created by chendong on 2016/12/25.
 */
public class HdfsUtils {

    final static String HDFS_URL = "hdfs://hadoop1:9000";
    final static String HADOOP_USER_NAME = "hadoop";


    static FileSystem fs = null;
    static Configuration conf = null;


    /**
     * hadoop配置初始化 获取hadoop文件系统操作类
     */
    static {
        conf = new Configuration();
        conf.set("fs.defaultFS", HDFS_URL);

        //拿到一个文件系统操作客户端是咧
        //fs = FileSystem.get(conf);

        //可以直接传入url和用户身份
        try {
            fs = FileSystem.get(new URI(HDFS_URL),conf,HADOOP_USER_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    public static void uploadFile(String localFilePath,String fsFilePath) throws IOException {
        fs.copyFromLocalFile(new Path(localFilePath),new Path(fsFilePath));
        fs.close();
    }

}
