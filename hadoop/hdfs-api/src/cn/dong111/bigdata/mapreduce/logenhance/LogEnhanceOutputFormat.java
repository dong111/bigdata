package cn.dong111.bigdata.mapreduce.logenhance;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * maptask或者reducetask在最终输出时,先调用OutputFormat的getRecordWriter方法拿到一个RecordWriter
 * 然后再调用RecordWriter的writer(k,v)方法将数据写出
 *
 * Created by chendong on 2016/12/29.
 */
public class LogEnhanceOutputFormat extends FileOutputFormat<Text,NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());

        Path enhancePath = new Path("/Users/chendong/学习/hadoop-test/logenhance/enhance.txt");
        Path tocrawPath = new Path("/Users/chendong/学习/hadoop-test/logenhance/crawl.txt");

        FSDataOutputStream enhancedOs = fs.create(enhancePath);
        FSDataOutputStream tocrawOs = fs.create(tocrawPath);

        return new EnhanceRecordWriter(enhancedOs,tocrawOs);
    }

    /**
     * 构造一个自己的recordwriter
     */
    static class EnhanceRecordWriter extends  RecordWriter<Text,NullWritable>{
        FSDataOutputStream enhanceOs = null;
        FSDataOutputStream tocrewlOs = null;

        public EnhanceRecordWriter(FSDataOutputStream enhanceOs,FSDataOutputStream tocrewlOs){
            this.enhanceOs = enhanceOs;
            this.tocrewlOs = tocrewlOs;
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            String result = key.toString();
            //如果要写出数据是待爬的url,则写入待爬清单文件  /logenhance/tocrawl/url.dat
            if(result.contains("tocrawl")){
                tocrewlOs.write(result.getBytes());
            }else {
                //如果要写出的数据是曾强日志,则吸入增强日志文件  /logenhance/enhancelog/log.dat
                enhanceOs.write(result.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if(tocrewlOs!=null){
                tocrewlOs.close();
            }
            if(enhanceOs!=null){
                enhanceOs.close();
            }
        }
    }

}
