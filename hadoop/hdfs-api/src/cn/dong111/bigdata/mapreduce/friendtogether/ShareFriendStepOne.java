package cn.dong111.bigdata.mapreduce.friendtogether;

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
 * 统计共同好友
 * step one 梳理出每个人的好友,去掉重复数据  (观察左边数据没有重复,右边数据由重复,右边数据作为reducer的key去去重复)
 *原始数据:
 A:B,C,D,F,E,O,B
 B:A,C,E,K
 C:F,A,D,I
 D:A,E,F,L
 E:B,C,D,M,L
 F:A,B,C,D,E,O,M
 G:A,C,D,E,F
 H:A,C,D,E,O
 I:A,O
 J:B,O
 K:A,C,D
 L:D,E,F
 M:E,F,G
 O:A,H,I,J
 *
 *输出结果
 A	I,K,C,B,G,F,H,O,D,
 B	A,F,J,E,
 C	A,E,B,H,F,G,K,
 D	G,C,K,A,L,F,E,H,
 E	G,M,L,H,A,F,B,D,
 F	L,M,D,C,G,A,
 G	M,
 H	O,
 I	O,C,
 J	O,
 K	B,
 L	D,E,
 M	E,F,
 O	A,H,I,J,F,
 * Created by chendong on 2016/12/27.
 */
public class ShareFriendStepOne {

    static class SharedFriendsStepOneMaper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] person_firends = line.split(":");
            String person = person_firends[0];
            String[] friends = person_firends[1].split(",");
            for (String friend :friends){
                //输出<好友,人>
                context.write(new Text(friend),new Text(person));
            }
        }
    }


    static class ShareFriendsStepOneReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text person : persons) {
                sb.append(person).append(",");
            }
            context.write(friend, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/friendtogeter/input/";
        final  String LOCAL_OUTPUT_PATH = "/friendtogeter/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ShareFriendStepOne.class);

        job.setMapperClass(SharedFriendsStepOneMaper.class);
        job.setReducerClass(ShareFriendsStepOneReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));;

        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }



}
