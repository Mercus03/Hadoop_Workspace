/**
 * 84116
 *2018年10月12日
 */
package com.myhadoop.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
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
        
        //告诉Job我们自定义了分区功能
        job.setPartitionerClass(ProviderPartitioner.class);
        
        
        job.setReducerClass(DCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //设置Reducer的数量，默认情况下只启动一个Reducer，一个Reducer对应一个文件，
        //我们现在想要得到4个文件，自然而然我们得启动多个Reducer，
        //为了程序的灵活性我们通过参数的形式给它赋值。
        job.setNumReduceTasks(Integer.parseInt(args[2]));
        
        
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
		protected void reduce(Text key, Iterable<DataBean> v2s, Reducer<Text, DataBean, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long up_sum=0;
			long down_sum=0;
			for(DataBean bean:v2s) {
				up_sum+=bean.getUpPayLoad();
				down_sum+=bean.getDownPayLoad();
			}
			DataBean bean = new DataBean(key.toString(),up_sum,down_sum);
			context.write(key, bean);
		}
    	
    }
    
    public static class ProviderPartitioner extends Partitioner<Text,DataBean>{
    	 /**
         * numPartitions---分区的值是由Reducer的数量决定的，起几个Reducer就创建几个分区
         */
    	public static Map<String,Integer> providerMap= new HashMap<String,Integer>();

    	static {
    		providerMap.put("135", 1);
    		providerMap.put("136", 1);
    		providerMap.put("137", 1);
    		providerMap.put("138", 1);
    		providerMap.put("139", 1);
    		providerMap.put("150", 2);
    		providerMap.put("159", 2);
    		providerMap.put("182", 3);
    		providerMap.put("183", 3);
    	}
    	
		@Override
		public int getPartition(Text key, DataBean dataBean, int numPartitions) {
			// TODO Auto-generated method stub
			//key是电话号码
			String telNo = key.toString();
			//我们截取前3位
			//通过前三位就可以知道是移动、联通还是电信或是其它
			String sub_tel = telNo.substring(0, 3);
			Integer num = providerMap.get(sub_tel);
			if(num==null) {
				return 0;
			}
			return num;
		}
    }
    
}
