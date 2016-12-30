package cn.dong111.bigdata.mapreduce.logenhance;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by chendong on 2016/12/29.
 */
public class LogEnhance {


    static  class LogEnhanceMaper extends Mapper<LongWritable,Text,Text,NullWritable>{

        Map<String, String> ruleMap = new HashMap<String, String>();

        Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                DBLoader.dbLoader(ruleMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取一个计数器用来记录不合法的日志行数,组名,计数器名称
            Counter counter = context.getCounter("malformed","malformedline");
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            try {
                String url = fields[26];
                String content_tag = ruleMap.get(url);
                if(content_tag==null){
                    k.set(url+"\t"+"tocrawl"+"\n");
                    context.write(k,v);
                }else {
                    k.set(url+"\t"+content_tag+"\n");
                    context.write(k,v);
                }

            }catch (Exception exception){
                counter.increment(1);
            }

        }
    }


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/logenhance/input/";
        final  String LOCAL_OUTPUT_PATH = "/logenhance/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEnhance.class);

        job.setMapperClass(LogEnhanceMaper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //要控制不同的内容写往不同的目标路径,可以采用自定义outputformat的方法
        job.setOutputFormatClass(LogEnhanceOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));

        //尽管我们用的是自定义outputformat,但是它是继承fileoutputormat
        //在fileoutputformat中,必须输出一个_SUCCESS文件,所以在此还需要设置输出path
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));;

        //不需要reducer
        job.setNumReduceTasks(0);

        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
