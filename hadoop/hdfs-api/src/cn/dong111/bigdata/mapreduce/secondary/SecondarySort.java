package cn.dong111.bigdata.mapreduce.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 输入数据:
 Order_0000001,Pdt_01,222.8
 Order_0000001,Pdt_05,25.8
 Order_0000002,Pdt_05,325.8
 Order_0000002,Pdt_03,522.8
 Order_0000002,Pdt_04,122.4
 Order_0000003,Pdt_01,222.8
 *
 * Created by chendong on 2016/12/28.
 */
public class SecondarySort {
    static class  SecondarySortMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
        OrderBean bean = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            bean.set(new Text(fields[0]),new DoubleWritable(Double.parseDouble(fields[2])));

            context.write(bean, NullWritable.get());
        }
    }


    static class  SecondarySortReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{
        //到达reducer时,相同id的所有bean已经看成一组,且金额最大的那个排在第一位
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/secondarysort/input/";
        final  String LOCAL_OUTPUT_PATH = "/secondarysort/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));


        //在此设置自定义的GroupingComparator类
        job.setGroupingComparatorClass(ItemIdGroupingComparator.class);
        //在此设置自定义的partitioner类
        job.setPartitionerClass(ItemIdPartitioner.class);

        job.setNumReduceTasks(2);

        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }



}
