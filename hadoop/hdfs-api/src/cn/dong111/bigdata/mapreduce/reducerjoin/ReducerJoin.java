package cn.dong111.bigdata.mapreduce.reducerjoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * 在Reducer中实现订单和商品的关联关系
 * order.txt(订单id, 日期, 商品编号, 数量)
 1001,20150710,P0001,2
 1002,20150710,P0001,3
 1002,20150710,P0002,3
 1003,20150710,P0003,3
 product.txt(商品编号, 商品名字, 价格, 数量)
 P0001	小米5	1001	2
 P0002	锤子T1	1000	3
 P0003	锤子	1002	4
 * Created by chendong on 2016/12/25.
 */
public class ReducerJoin {

    static class RJoinMapper extends Mapper<LongWritable,Text,Text,OrderProductBean>{

        OrderProductBean bean = new OrderProductBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取每一行数据
            String line = value.toString();
            //通过文件名来判断是order还是product
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String name = inputSplit.getPath().getName();
            String pid = "";
            if(name.startsWith("order")){
                String[] fields = line.split(",");
                pid = fields[2];
                bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");
            }else{
                String[] fields = line.split(",");
                pid = fields[0];
                bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");
            }
            k.set(pid);
            context.write(k,bean);
        }
    }



    static class RJoinReducer extends Reducer<Text,OrderProductBean,OrderProductBean,NullWritable>{


        @Override
        protected void reduce(Text key, Iterable<OrderProductBean> values, Context context) throws IOException, InterruptedException {
            OrderProductBean pdBean = new OrderProductBean();//商品唯一
            ArrayList<OrderProductBean> orderBeans = new ArrayList<OrderProductBean>();

            for(OrderProductBean bean :values){
                if(StringUtils.equals("1",bean.getFlag())){//产品
                    try {
                        BeanUtils.copyProperties(pdBean, bean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }else{
                    OrderProductBean odbean = new OrderProductBean();
                    try {
                        BeanUtils.copyProperties(odbean, bean);
                        orderBeans.add(odbean);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            // 拼接两类数据形成最终结果
            for (OrderProductBean bean : orderBeans) {

                bean.setPname(pdBean.getPname());
                bean.setCategory_id(pdBean.getCategory_id());
                bean.setPrice(pdBean.getPrice());

                context.write(bean, NullWritable.get());
            }

        }
    }



    public static void main(String[] args) throws Exception {
        //定义hdfs文件系统请求地址
        final  String LOCAL_INPUT_PATH = "/rjoin/input/";
        final  String LOCAL_OUTPUT_PATH = "/rjoin/output";
        final  String HADOOP_USER_NAME = "hadoop";
        final  String LOCAL_FILE_PATH = "file:///Users/chendong/学习/hadoop-test/";
        //设置程序运行角色
        System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME);
        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = LOCAL_FILE_PATH+LOCAL_INPUT_PATH;
            args[1] = LOCAL_FILE_PATH+LOCAL_OUTPUT_PATH;
        }


        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(RJoin.class);
//		job.setJar("c:/join.jar");

        job.setJarByClass(ReducerJoin.class);
        // 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderProductBean.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(OrderProductBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }


}
