package com.ebusiness.report.outputformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.ebusiness.report.ReportOutputTuple;

/**
 * 自定义输出格式，把数据输出到关系数据库
 * @author Administrator
 *
 */
public class DBOutputFormat extends OutputFormat<IntWritable, ReportOutputTuple> {

	/**
	 * 检查必要的输出条件是否满足要求
	 */
	@Override
	public void checkOutputSpecs(JobContext context) throws IOException,
			InterruptedException {
		/**
		 * 可以做一些数据库连接检测
		 */
		
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		return new NullOutputFormat<IntWritable, ReportOutputTuple>().getOutputCommitter(context);
	}

	@Override
	public RecordWriter<IntWritable, ReportOutputTuple> getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new DBRecordWriter();
	}

}
