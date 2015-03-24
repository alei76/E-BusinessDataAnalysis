package com.ebusiness.assist.tobayesformat;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ebusiness.etl.transform.ITransformer;
import com.ebusiness.etl.transform.JsonTransformer;

public class BayesFormatMapper extends Mapper<Object, Text, IntWritable, Text> {

	private IntWritable categoryId = new IntWritable();
	
	private Text outValue = new Text();
	
	private ITransformer transformer = new JsonTransformer();
	
	private HashMap<String, String> dataContainer = null ;
	
	private static String REPORT_GROUP = "REPORT_GROUP";
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		dataContainer = transformer.getFormatData(value.toString());

		if (dataContainer == null)
			return;

		if (!StringUtils.isEmpty(dataContainer.get("category"))) {
			categoryId.set(Integer.parseInt(dataContainer.get("category")));
		} else {
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		if(!StringUtils.isEmpty(dataContainer.get("name"))){
			outValue.set(dataContainer.get("name"));
		}else{
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		/**
		 * 把类别标识符和相应的商品名称描述输出
		 */
		context.write(categoryId, outValue);

	}
}
