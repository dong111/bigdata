package cn.dong111.bigdata.mapreduce.flowsum;

import cn.dong111.bigdata.hdfs.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 远程yarn模式下运行程序
 * Created by chendong on 2016/12/25.
 */
public class FlowCountYarnClient {

    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String HDFS_URL = "hdfs://hadoop1:9000";
        final  String HDFS_INPUT_PATH = "/flowcount/input/";
        final  String HDFS_OUTPUT_PATH = "/flowcount/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);
        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = HDFS_URL+HDFS_INPUT_PATH;
            args[1] = HDFS_URL+HDFS_OUTPUT_PATH;
        }
        /**
         * 上传需要处理的文件信息
         */
        HdfsUtils.uploadFile(LOCAL_FILE_PATH + "flow.log", HDFS_INPUT_PATH + "flow.log");

        /**
         * 构建mr任务提交客户端
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定任务需要提交的jar所在的目录
        job.setJar(LOCAL_FILE_PATH+"flowcount.jar");
        //指定本程序的jar包所在的本地路径
//        job.setJarByClass(FlowCount.class);

        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
		/*conf.set("mapreduce.framework.name", "local");*/

        //本地模式运行mr程序时，输入输出的数据可以在本地，也可以在hdfs上
        //到底在哪里，就看以下两行配置你用哪行，默认就是file:///
		/*conf.set("fs.defaultFS", "hdfs://mini1:9000/");*/
		/*conf.set("fs.defaultFS", "file:///");*/

        conf.set("HADOOP_USER_NAME",HADOOP_USER_NAME);

        //???为什么没有生效呢?只有将hadoop*-site.xml放到classpath才生效
//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","hadoop1");
//        conf.set("fs.defaultFS",HDFS_URL);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCount.FlowCountMapper.class);
        job.setReducerClass(FlowCount.FlowCountReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }


}
