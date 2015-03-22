package com.ebusiness.report;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.ebusiness.report.mr.mapper.ReportMapper;
import com.ebusiness.report.mr.reducer.reportReducer;

public class ReportDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage:  <in> <out>");
			System.exit(2);
		}

		Path outputPath = new Path(args[1]);
		FileSystem outfs = outputPath.getFileSystem(conf);
		// 这里是为了调试的时候不必每次都手动删除输出文件
		if (outfs.exists(outputPath)) {
			try {
				outfs.delete(outputPath, true);
			} catch (Exception e) {
				e.printStackTrace();
			}
			outfs.close();
		}

		Job job = Job.getInstance(conf, "ebusinessReport");
		job.setJarByClass(ReportDriver.class);
		job.setMapperClass(ReportMapper.class);
		//job.setCombinerClass(reportReducer.class);
		job.setReducerClass(reportReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ReportOutputTuple.class);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
