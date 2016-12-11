package cn.dong111.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

/**
 * 操作hdsf客户端-java-api实例类
 * <p/>
 * 注意：运行此demo 本地hadoop环境变量配置
 * window下开发的说明
 * 建议在linux下进行hadoop应用的开发，不会存在兼容性问题。如在window上做客户端应用开发，需要设置以下环境：
 * 在windows的某个目录下解压一个hadoop的安装包
 * 将安装包下的lib和bin目录用对应windows版本平台编译的本地库替换
 * 在window系统中配置HADOOP_HOME指向你解压的安装包
 * 在windows系统的path变量中加入hadoop的bin目录
 *
 * @author chendong
 */
public class HdfsClientDemo {

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
     * 使用java客户端命令上传一个本地文件
     * @throws Exception
     */
    @Test
    public void testUpload()throws Exception{
        fs.copyFromLocalFile(new Path("/Users/chendong/学习/temp/test1.txt"),
                new Path("/temp/text1.txt"));
        fs.close();
    }


    @Test
    public void testDownload()throws Exception{
        //使用java客户端命令下载一个文件到本地
        fs.copyToLocalFile(new Path("/temp/text1.txt")
                ,new Path("/Users/chendong/学习/temp/download1.txt"));
        fs.close();
    }



    @Test
    public void testConf(){
        //打印所有的配置信息
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey()+"--"+entry.getValue());
        }
    }

    @Test
    public void makeDirTest()throws Exception{
        //创建文件目录
        boolean mkdirs = fs.mkdirs(new Path("/bbb/bbb"));
        System.out.println(mkdirs);
    }


    @Test
    public void deleteTest()throws Exception{
        //删除目录 第二个参数是是否递归删除
        boolean delete = fs.delete(new Path("/aaa"), false);
        System.out.println(delete);
    }


    //列出所有文件目录
    @Test
    public void listTest()throws Exception{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus:fileStatuses){
            System.out.println(fileStatus.getPath()+"======"+fileStatus.toString());
        }
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            String name =next.getPath().getName();
            Path path =next.getPath();
            System.out.println(name+"===="+path.toString());
        }
    }


}
