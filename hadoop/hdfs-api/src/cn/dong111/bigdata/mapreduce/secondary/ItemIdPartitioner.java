package cn.dong111.bigdata.mapreduce.secondary;

import cn.dong111.bigdata.mapreduce.secondary.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义Partitioner
 目的:给mr程序指派分区实现(把业务相关的类交给同一个reducer执行)
 *
 * 本程序自定义的bean作为key,通过benid来
 *
 * Created by chendong on 2016/12/28.
 */
public class ItemIdPartitioner extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        //相同id的订单bean,会发往相同的partition
        //而且,产生的分区数,是会跟用户设置的reducer task保持一致

        return (orderBean.getItemid().hashCode()&Integer.MAX_VALUE)%numReduceTasks;
    }
}
