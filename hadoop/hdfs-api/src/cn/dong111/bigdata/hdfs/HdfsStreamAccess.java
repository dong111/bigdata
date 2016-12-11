package cn.dong111.bigdata.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * 用流的方式来操作hdfs上的文件
 * 可以实现读取指定偏移量范围的数据
 * Created by chendong on 2016/12/11.
 */
public class HdfsStreamAccess {

    FileSystem fs = null;
    Configuration conf = null;

    /**
     * hadoop配置初始化 获取hadoop文件系统操作类
     */
    @Before
    public void init() throws Exception{
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://slave1:9000");

        //拿到一个文件系统操作客户端是咧
        //fs = FileSystem.get(conf);

        //可以直接传入url和用户身份
        fs = FileSystem.get(new URI("hdfs://slave1:9000"),conf,"hadoop");
    }

    /**
     * 是用流的方式上传文件到hdfs
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception{
        FSDataOutputStream outputStream = fs.create(new Path("/tianxiawudi.txt"), true);
        FileInputStream inputStream = new FileInputStream("/Users/chendong/学习/temp/tianxiawudi.txt");
        IOUtils.copy(inputStream,outputStream);
        fs.close();
    }

    /**
     * 通过流的方式获取dnfs上的数据
     * @throws Exception
     */
    @Test
    public void testDownLoad()throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/tianxiawudi.txt"));
        FileOutputStream outputStream = new FileOutputStream(new File("/Users/chendong/学习/temp/tianxiawudi.txt"));
        IOUtils.copy(inputStream,outputStream);
    }


    @Test
    public void testRandomAccess()throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/tianxiawudi.txt"));

        //实现读取指定偏移量范围的数据
        inputStream.seek(3);

        FileOutputStream outputStream = new FileOutputStream(new File("/Users/chendong/学习/temp/tianxiawudi.txt"));
        IOUtils.copy(inputStream,outputStream);

        fs.close();
    }


    /**
     * 显示hdfs上文件内容到控制台
     * @throws Exception
     */
    @Test
    public void testCat()throws Exception{
        FSDataInputStream inputStream = fs.open(new Path("/tianxiawudi.txt"));
        IOUtils.copy(inputStream,System.out);
    }


}
