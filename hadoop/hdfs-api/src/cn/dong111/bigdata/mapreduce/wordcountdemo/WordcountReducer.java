package cn.dong111.bigdata.mapreduce.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN,VALUEIN,对应mapper输出的KEYOUT,VALUEOUT类型对应
 *
 * KEYOUT,VALUEOUT 是自定义reduce逻辑处理结果输出的数据类型
 * KEYOUT是单词
 * VALUEOUT是总次数
 * Created by chendong on 2016/12/12.
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * <word1,1><word1,1><word1,1><word1,1><word1,1><word1,1>
     *     <word2,1><word2,1><word2,1><word2,1><word2,1>
     * @param key 是一组相同单词kv对的key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;

        for(IntWritable value:values){
            count += value.get();
        }

        context.write(key,new IntWritable(count));

    }
}
