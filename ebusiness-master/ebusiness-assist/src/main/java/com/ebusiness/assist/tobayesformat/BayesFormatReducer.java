package com.ebusiness.assist.tobayesformat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BayesFormatReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	 @Override
	protected void reduce(IntWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		 /**
		  * 直接输出
		  */
		 for(Text value:values){
			context.write(key, value);
		}
	}
	
}
