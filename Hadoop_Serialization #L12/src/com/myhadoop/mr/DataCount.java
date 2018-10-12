/**
 * 84116
 *2018年10月12日
 */
package com.myhadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.ctc.wstx.dtd.ConcatModel;

/**
 * @author 84116
 *
 */
public class DataCount {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(DataCount.class);
        
        job.setMapperClass(DCMapper.class);
        job.
    }
    
    public static class DCMapper extends Mapper<LongWritable,Text,Text,DataBean>{
        Text text = null;
        @Override
        protected void map(LongWritable key,Text value,Context context) {
            String line = value.toString();
            String[] fields = line.split("\t");
            
            //我们要使用的数据的第二列（列索引号为1）就是手机号，
            //第9列（列索引号8）是上行流量，
            //第10列（列索引号9）是下行流量
            String telNo = fields[1];
            long up
        }
    }
    
}
