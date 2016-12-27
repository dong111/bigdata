package cn.dong111.bigdata.mapreduce.buildindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 构建索引第二步
 * 1.处理step one中输出的结果
 * step one 输出结果
 hello--a.txt	3
 hello--b.txt	2
 hello--c.txt	2
 jerry--a.txt	1
 jerry--b.txt	3
 jerry--c.txt	1
 tom--a.txt	2
 tom--b.txt	1
 tom--c.txt	1
 *
 * 2.最终统计结果,单词在不同文件中出现的次数
 hello	c.txt-->2 b.txt-->2 a.txt-->3
 jerry	c.txt-->1 b.txt-->3 a.txt-->1
 tom	c.txt-->1 b.txt-->1 a.txt-->2

 * Created by chendong on 2016/12/26.
 */
public class BuildIndexStepTwo {


    static  class IndexStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("--");
            String word = splits[0];
            String[] file_count = splits[1].split("\t");

            context.write(new Text(word),new Text(file_count[0]+"-->"+file_count[1]));

        }
    }



    static class IndexStepTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text value :values){
                sb.append(value.toString()).append(" ");
            }

            context.write(key,new Text(sb.toString()));

        }
    }


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/buildindex/output/";
        final  String LOCAL_OUTPUT_PATH = "/buildindex/output2/";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(BuildIndexStepTwo.class);

        job.setMapperClass(IndexStepTwoMapper.class);
        job.setReducerClass(IndexStepTwoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));;



        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }


}
