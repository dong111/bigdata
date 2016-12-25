package cn.dong111.bigdata.mapreduce.buildindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

/**
 * 构建索引第一步
 * 1.不同文件中数据如下
 * a.txt
 hello tom
 hello jerry
 hello tom
 * b.txt
 hello jerry
 hello jerry
 tom jerry
 * c.txt
 hello jerry
 hello tom
 *
 * 2.最终统计结果,单词在不同文件中出现的次数
 * hello a.txt-->3 b.txt-->2 c.txt-->2
 * jerry a.txt-->1 b.txt-->2 c.txt-->1
 *
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
 * Created by chendong on 2016/12/26.
 */
public class BuildIndexStepOne {

    static class IndexStepOneMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(" ");

            //获取文件名称
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();


            for (String word:words){
                context.write(new Text(word+"--"+fileName),new IntWritable(1));
            }



        }
    }



    static class IndexStepOneReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sumCount = 0;

            for (IntWritable count:values){
                sumCount += count.get();
            }

            context.write(key,new IntWritable(sumCount));
        }
    }





    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/buildindex/input/";
        final  String LOCAL_OUTPUT_PATH = "/buildindex/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(BuildIndexStepOne.class);

        job.setMapperClass(IndexStepOneMapper.class);
        job.setReducerClass(IndexStepOneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));;



        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }









}
