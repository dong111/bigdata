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
 * 知道 好友  人 ,统计 人-人 的共同好友
 * 输入数据
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
 *
 * 输出结果
 *
 ……
 A-J	O B
 A-K	C
 A-L	D
 B-D	E A
 B-F	C A
 ……
 *
 * Created by chendong on 2016/12/27.
 */
public class ShareFriendsStepTwo {
    static class  ShareFriendsStepTwoMapper extends Mapper<LongWritable,Text,Text,Text>{
        // 拿到的数据是上一个步骤的输出结果
        // A I,K,C,B,G,F,H,O,D,
        // 友 人，人，人

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_persons = line.split("\t");

            String friend = friend_persons[0];

            String[] persons = friend_persons[1].split(",");

            for (int i = 0; i < persons.length - 1; i++) {
                for (int j = i + 1; j < persons.length; j++) {
                    // 发出 <人-人，好友> ，这样，相同的“人-人”对的所有好友就会到同1个reduce中去
                    context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
                }

            }
        }
    }


    static class  ShareFriendsStepTwoReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text person_person, Iterable<Text> friends, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();

            for (Text friend : friends) {
                sb.append(friend).append(" ");

            }
            context.write(person_person, new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/friendtogeter/output/";
        final  String LOCAL_OUTPUT_PATH = "/friendtogeter/output2";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ShareFriendsStepTwo.class);

        job.setMapperClass(ShareFriendsStepTwoMapper.class);
        job.setReducerClass(ShareFriendsStepTwoReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(LOCAL_FILE_PATH + LOCAL_INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL_FILE_PATH + LOCAL_OUTPUT_PATH));;

        boolean res =job.waitForCompletion(true);
        System.exit(res?0:1);
    }



}
