package com.hadoop.mr;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 84116
 *2018年9月29日
 */

/**
 * @author 84116
 *
 */
public class WCReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

    @Override
    protected void reduce(Text key, Iterable<LongWritable> arg1,
            Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
      //定义一个counter用来统计某个单词出现的次数是多少
        long counter=0;
        //其实v2s当中存储的都是一个个被序列化好了的1
        for(LongWritable i : arg1){
            counter+=i.get();//跟我们熟悉的counter++是一个意思
        }
        //输出<K3、V3>，比如<"hello", 5>
        context.write(key, new LongWritable(counter));

   }
    
}
