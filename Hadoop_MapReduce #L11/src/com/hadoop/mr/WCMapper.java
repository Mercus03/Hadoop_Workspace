package com.hadoop.mr;
/**
 * 84116
 *2018年9月29日
 */

/**
 * @author 84116
 *
 */


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 定义一个Mapper类，该类继承自Hadoop的Mapper类，Mapper类有4个泛型，分别代表
 * KEYIN(K1)、VALUEIN(V1)、KEYOUT(K2)、VALEOUT(V2)，其中<K1,V1>的数据如：
 * <0, "hello tom">，<K2,V2>的数据如：<"hello", 1>。Mapper的这4个泛型一定要实现
 * 序列化，这样方便快速传输。Hadoop所用的序列化与jdk所用的序列化是不一样的，因为
 * jdk的序列化机制非常冗余(需要保存类之间的关系等)，因此Hadoop实现了自己的一套序
 * 列化机制，其中数值型的数据可以用LongWritable来序列化，字符串型的数据可以用Text
 * 来序列化。我们发现K1是数值类型，因此它的序列化泛型是LongWritable，V1是字符串因
 * 此它的序列化泛型是Text，K2是字符串类型，因此它的序列化泛型是Text，V2是数值类型
 * 因此它的序列化泛型是LongWritable。
 * 
 * @author 
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
  //需要重写map方法
  @Override
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
          throws IOException, InterruptedException {
      //接收数据V1
      String line = value.toString();
      //切分数据
      String[] words = line.split(" ");
      //循环输出word
      for(String word : words){
          //由于word是String类型数据，没有序列化，因此在写出去之前先序列化。
          //1是int类型，没有序列化，因此要序列化。
          context.write(new Text(word), new LongWritable(1));
      }
  }

}

