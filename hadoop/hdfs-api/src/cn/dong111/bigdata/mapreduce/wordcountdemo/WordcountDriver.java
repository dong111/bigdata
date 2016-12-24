package cn.dong111.bigdata.mapreduce.wordcountdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数,指定jar包
 * 最后提交到yarn
 * Created by chendong on 2016/12/12.
 */
public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            if (args == null || args.length == 0) {
                    args = new String[2];
                    args[0] = "file:///Users/chendong/学习/hadoop-test/input";
                    args[1] = "file:///Users/chendong/学习/hadoop-test/output";
//                    args[0] = "hdfs://hadoop1:9000/wordcount/input/";
//                    args[1] = "hdfs://hadoop1:9000/wordcount/output1";
            }
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        //只设置conf中的用户是没有用的,
        // 还需要设置java运行系统用户名,并且在conf设置之前.System.setProperty("HADOOP_USER_NAME", "hadoop");
		conf.set("HADOOP_USER_NAME", "hadoop");


//        conf.set("mapreduce.framework.name","yarn");
//        conf.set("yarn.resourcemanager.hostname","hadoop1");
//        conf.set("fs.defaultFS","hdfs://hadoop1:9000/");


//        job.setJar("/home/hadoop/wc.jar");
        //指定本程序的Jar所在的本地路径
        job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定最终输出的数据kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定job的输入原始文件所在的目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        //指定job输出结果所在的目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));;

        //将job中配置的相关参数,以及job所用的java类所在的jar包,提交给yarn去运行
//        job.submit();
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }


}
