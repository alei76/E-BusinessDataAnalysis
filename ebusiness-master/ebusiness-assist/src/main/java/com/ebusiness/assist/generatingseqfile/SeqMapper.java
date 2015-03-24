package com.ebusiness.assist.generatingseqfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class SeqMapper extends Mapper<IntWritable, Text, IntWritable, VectorWritable> {

	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}
}
