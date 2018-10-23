package com.myhadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumStep {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SumStep.class);

		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

	public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		private Text k = new Text();
		private InfoBean v = new InfoBean();

		/**
		 * 重写map方法
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 首先获取一行的数据
			String line = value.toString();
			// 由于我们的数据是以空格分割的，因此我们以空格进行分割
			String[] fields = line.split("\t");
			// 获取帐号
			String account = fields[0];
			// 获取收入的值
			double income = Double.parseDouble(fields[1]);
			// 获取支出的值
			double outcome = Double.parseDouble(fields[2]);
			// 给k赋值
			k.set(account);
			// 给v赋值
			v.set(account, income, outcome);
			// 输出
			context.write(k, v);
		}
	}

	public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
		private InfoBean v = new InfoBean();

		/**
		 *          * 重写Reduce方法          
		 */
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values, Context context)
				throws IOException, InterruptedException {
			// 每个账户总的收入
			double sum_income = 0;
			// 每个账户总的支出
			double sum_outcome = 0;
			for (InfoBean bean : values) {
				sum_income += bean.getIncome();
				sum_outcome += bean.getOutcome();
			}
			// 由于key就是账户，因此这里不用传account进去也没问题
			v.set("", sum_income, sum_outcome);
			// 输出
			context.write(key, v);
		}
	}
}
