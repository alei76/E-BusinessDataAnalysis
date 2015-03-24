package com.ebusiness.report.mr.mapper;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.ebusiness.etl.transform.ITransformer;
import com.ebusiness.etl.transform.JsonTransformer;
import com.ebusiness.report.ReportOutputTuple;

public class ReportMapper extends Mapper<Object, Text, IntWritable,ReportOutputTuple> {

	
	private IntWritable categoryId = new IntWritable();
	
	private ReportOutputTuple reportOutputTuple = new ReportOutputTuple();
	
	private ITransformer transformer = new JsonTransformer();
	
	private HashMap<String, String> dataContainer = null ;
	
	private static String REPORT_GROUP = "REPORT_GROUP";
	
	@Override
	protected void map(Object key, Text value,Context context)
			throws IOException, InterruptedException {
		
		if(StringUtils.isEmpty(value.toString())){
			return ;
		}
		
		dataContainer = transformer.getFormatData(value.toString());
		
		if(dataContainer==null) return ;
		
		if(!StringUtils.isEmpty(dataContainer.get("category"))){
			categoryId.set(Integer.parseInt(dataContainer.get("category")));	
		}else{
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		if(!StringUtils.isEmpty(dataContainer.get("price"))){
			reportOutputTuple.setMaxPrice(Double.parseDouble(dataContainer.get("price")));	
			reportOutputTuple.setMinPrice(Double.parseDouble(dataContainer.get("price")));
			reportOutputTuple.setAveragePrice(Double.parseDouble(dataContainer.get("price")));
		}else{
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		if(!StringUtils.isEmpty(dataContainer.get("query"))){
		//	reportOutputTuple.setQuery(dataContainer.get("query"));
		}else{
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		reportOutputTuple.setCount(1);
		
		context.write(categoryId, reportOutputTuple);
	}
}
