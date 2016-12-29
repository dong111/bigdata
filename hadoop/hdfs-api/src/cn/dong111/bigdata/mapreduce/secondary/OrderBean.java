package cn.dong111.bigdata.mapreduce.secondary;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 实现接口WritableComparable
 目的:实现实体对象序列化和大小的比较
 *
 *
 * Created by chendong on 2016/12/28.
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private Text itemid;
    private DoubleWritable amount;

    public OrderBean() {
    }

    public OrderBean(Text itemid, DoubleWritable amount) {
        set(itemid, amount);

    }


    public void set(Text itemid, DoubleWritable amount) {

        this.itemid = itemid;
        this.amount = amount;

    }

    /**
     * 比较对象大小
     * @param o
     * @return
     */
    @Override
    public int compareTo(OrderBean o) {
        //先根据id比大小,
        int cmp = this.itemid.compareTo(o.getItemid());
        if (cmp == 0) {
            cmp = -this.amount.compareTo(o.getAmount());
        }
        return cmp;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(itemid.toString());
        dataOutput.writeDouble(amount.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String readUTF = dataInput.readUTF();
        double amount = dataInput.readDouble();
        this.itemid = new Text(readUTF);
        this.amount = new DoubleWritable(amount);
    }



    public Text getItemid() {
        return itemid;
    }

    public DoubleWritable getAmount() {
        return amount;
    }

    @Override
    public String toString() {
        return itemid.toString() + "\t" + amount.get();
    }
}
