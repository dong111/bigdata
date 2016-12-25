package cn.dong111.bigdata.mapreduce.flowsum;

import cn.dong111.bigdata.hdfs.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 提交任务到
 * Created by chendong on 2016/12/25.
 */
public class FlowCountLocalClient {


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/flowcount/input/";
        final  String LOCAL_OUTPUT_PATH = "/flowcount/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);
        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = LOCAL_FILE_PATH+LOCAL_INPUT_PATH;
            args[1] = LOCAL_FILE_PATH+LOCAL_OUTPUT_PATH;
        }

        /**
         * 构建mr任务提交客户端
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountLocalClient.class);

        //是否运行为本地模式，就是看这个参数值是否为local，默认就是local
        conf.set("mapreduce.framework.name", "local");
        conf.set("HADOOP_USER_NAME",HADOOP_USER_NAME);



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
