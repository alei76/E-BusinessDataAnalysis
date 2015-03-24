package com.ebusiness.assist.tobayesformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.mahout.common.HadoopUtil;


public class ByesFormatDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}

		Path outputPath = new Path(args[1]);
		// 这里是为了调试的时候不必每次都手动删除输出文件
		HadoopUtil.delete(conf, outputPath);
		Job job = Job.getInstance(conf, "ebusinessReport");
		job.setJarByClass(ByesFormatDriver.class);
		job.setMapperClass(BayesFormatMapper.class);
		job.setReducerClass(BayesFormatReducer.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
