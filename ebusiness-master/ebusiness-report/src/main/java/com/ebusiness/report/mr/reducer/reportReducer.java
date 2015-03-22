package com.ebusiness.report.mr.reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.ebusiness.report.ReportOutputTuple;

public class reportReducer extends Reducer<IntWritable, ReportOutputTuple, IntWritable, ReportOutputTuple> {

	
	ReportOutputTuple outTuple = new ReportOutputTuple();
	
	@Override
	protected void reduce(IntWritable key,Iterable<ReportOutputTuple> values,Context context)
			throws IOException, InterruptedException {
	
		outTuple.setAveragePrice(0);
		outTuple.setCategoryId(0);
		outTuple.setCount(0);
		outTuple.setMaxPrice(0);
		outTuple.setMinPrice(0);
		/**
		 * 总记录数
		 */
		long count = 0;
		

		BigDecimal totlePrice =new BigDecimal(0);
		
		
		BigDecimal max = null;
		BigDecimal min = null;
		BigDecimal avg =new BigDecimal(0);
		
		Iterator<ReportOutputTuple> It =  values.iterator();
		
		while(It.hasNext()){
			ReportOutputTuple inTuple = It.next();
			//计数值求和
			count+= inTuple.getCount();
			BigDecimal  tmpMax = BigDecimal.valueOf(inTuple.getMaxPrice());
			
			if(tmpMax.intValue()>9999999){
				System.out.println(9999999);
			}
			
			if(max==null||max.compareTo(tmpMax)<0){
				max =tmpMax;
			}
			
			BigDecimal  tmpMin = BigDecimal.valueOf(inTuple.getMinPrice());
			if(tmpMin.intValue()==0.01){
				System.out.println(tmpMin.intValue());
			}
			if(min==null||min.compareTo(tmpMin)>0){
				min=tmpMin;
			}
			
			totlePrice =totlePrice.add(BigDecimal.valueOf(inTuple.getCount()*inTuple.getAveragePrice()));
		}
		
		avg = totlePrice.divide(BigDecimal.valueOf(count),3);
		outTuple.setCategoryId(key.get());
		outTuple.setAveragePrice(avg.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
		outTuple.setCount(count);
		outTuple.setMaxPrice(max.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
		outTuple.setMinPrice(min.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
		
		context.write(key, outTuple);
	}
}
