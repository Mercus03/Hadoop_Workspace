/**
 * 84116
 *2018年10月12日
 */
package com.myhadoop.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author 84116
 *
 */
public class DataCount {

    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        
        job.setJarByClass(DataCount.class);
        
        job.setMapperClass(DCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        
        
        
        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //FileInputFormat.setInputPaths(job, new Path(args[0]));
    
        job.waitForCompletion(true);
    }
    
    public static class DCMapper extends Mapper<LongWritable,Text,Text,DataBean>{
        Text text = null;
        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            
            //我们要使用的数据的第二列（列索引号为1）就是手机号，
            //第9列（列索引号8）是上行流量，
            //第10列（列索引号9）是下行流量
            String telNo = fields[1];
            long up = Long.parseLong(fields[8]);
            long down = Long.parseLong(fields[9]);
            DataBean bean = new DataBean(telNo,up,down);
            text = new Text(telNo);
            context.write(text, bean);
        }
    }
    
    public static class DCReducer extends Reducer<Text,DataBean,Text,DataBean>{

		@Override
		protected void reduce(Text arg0, Iterable<DataBean> arg1, Reducer<Text, DataBean, Text, DataBean>.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, arg2);
		}

    	
//		@Override
//		protected void reduce(Text key, Iterable<DataBean> v2s, Reducer<Text, DataBean, Text, DataBean>.Context context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			long up_sum=0;
//			long down_sum=0;
//			for(DataBean bean:v2s) {
//				up_sum+=bean.getUpPayLoad();
//				down_sum+=bean.getDownPayLoad();
//			}
//			DataBean bean = new DataBean(key.toString(),up_sum,down_sum);
//			context.write(key, bean);
//		}
    	
    }
    
}
