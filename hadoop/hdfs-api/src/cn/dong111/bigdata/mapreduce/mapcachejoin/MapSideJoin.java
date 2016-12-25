package cn.dong111.bigdata.mapreduce.mapcachejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 在Map中建立order和product的join操作(消除reducer中数据倾斜问题)
 * 实现原理通过将产品表文件缓存到task工作节点的工作目录中去 去做join
 *
 * Created by chendong on 2016/12/25.
 */
public class MapSideJoin {
    static class MapSideJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        //用一个hashmap来加载保存的产品信息
        Map<String,String> pdInfoMap = new HashMap<String,String>();

        Text k= new Text();
        /**
         * 通过阅读父类的Mapper的源码,发现setup方法是在maptask处理数据之前调用一次,可以用来做map方法执行前的初始化工作
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("pdts.txt")));
            String line;
            while (StringUtils.isNotEmpty(line=br.readLine())){
                String[] fields = line.split(",");
                pdInfoMap.put(fields[0],fields[1]);
            }
            br.close();
        }

        //由于已经持有完整的产品信息表,所有在map方法中就能实现join的逻辑了
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String orderLine = value.toString();
            String[] fields = orderLine.split(",");
            String pdName = pdInfoMap.get(fields[2]);
            k.set(orderLine+"\t"+pdName);
            context.write(k, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/mapjoin/input/";
        final  String LOCAL_OUTPUT_PATH = "/mapjoin/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(MapSideJoinMapper.class);

        job.setMapperClass(MapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(LOCAL_FILE_PATH+LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(LOCAL_FILE_PATH+LOCAL_OUTPUT_PATH));;

        //指定需要缓冲一个文件到所有的maptask运行节点工作目录
//        job.addArchiveToClassPath(archive); //缓存jar包到task运行节点的classpath
//        job.addFileToClassPath(file);     //缓存普通文件到task运行节点的classpath
//        job.addCacheArchive(url);         //缓存压缩包文件到task运行节点的工作目录
//        job.addCacheFile(url);            //缓存普通文件到task运行的工作节点

        //将产品表文件缓存到task工作节点的工作目录中区
        job.addCacheFile(new URI(LOCAL_FILE_PATH+"/mapjoin/pdts.txt"));

        //map端join的逻辑不需要reduce阶段,设置reducertask数量为0
        job.setNumReduceTasks(0);

        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
