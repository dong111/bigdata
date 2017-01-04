package cn.dong111.bigdata.hdfs.HA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * (hadoop的HA环境下hdfs使用)
 * 两个namenode
 *
 * 直接运行报错
 * java.lang.IllegalArgumentException: java.net.UnknownHostException: bi
 *
 * 处理方式很简单:将HA的core-site.xml hdfs mapred yarn 的配置文件copy到classpath即可
 *
 * Created by chendong on 2017/1/4.
 */
public class TestHA {

    public static void main(String[] args) throws IOException {
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bi");

        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path("/Users/chendong/学习/hadoop-test/wordcount.jar")
                ,new Path("/"));
        fs.close();

    }

}
