package cn.dong111.bigdata.mapreduce.flowsum;

import cn.dong111.bigdata.hdfs.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by chendong on 2016/12/25.
 */
public class FlowCount {


    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行内容转成String
            String line = value.toString();
            //切分字段
            String[] fields = line.split("\t");
            //取出手机号
            String phone = fields[1];
            //取出上传和下载流量
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long dFlow = Long.parseLong(fields[fields.length - 2]);

            context.write(new Text(phone),new FlowBean(upFlow,dFlow));
        }
    }


    static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long sum_upFlow = 0;
            long sum_dFow = 0;

            //遍历相同号码的所有bean,将不同记录的上行和下行流量分别累加
            for (FlowBean flowBean:values){
                sum_upFlow += flowBean.getUpFlow();
                sum_dFow += flowBean.getdFlow();
            }

            FlowBean onePhoneBean = new FlowBean(sum_upFlow,sum_dFow);
            context.write(key,onePhoneBean);

        }
    }

















}
