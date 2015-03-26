package com.ebusiness.assist.tobayesformatseq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.StringTuple;

public class BayesFormatReducer extends Reducer<Text, StringTuple, Text, StringTuple> {
	 @Override
	protected void reduce(Text key, Iterable<StringTuple> values,Context context)
			throws IOException, InterruptedException {
		 /**
		  * 直接输出
		  */
		 for(StringTuple value:values){
			context.write(key, value);
		}
	}
	
}
