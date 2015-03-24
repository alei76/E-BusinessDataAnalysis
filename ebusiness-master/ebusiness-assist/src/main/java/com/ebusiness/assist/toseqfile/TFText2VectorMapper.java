package com.ebusiness.assist.toseqfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TFText2VectorMapper extends Mapper<IntWritable, Text, IntWritable, VectorWritable> {

	@Override
	protected void map(IntWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		/**
		 * 以#来分割文本的特征词
		 */
		String[] values = value.toString().split("#");
		
		/**
		 *构建随机存储稀疏向量 
		 */
		Vector sparseVector =new RandomAccessSparseVector(values.length);  
	
		
		
	}
}
