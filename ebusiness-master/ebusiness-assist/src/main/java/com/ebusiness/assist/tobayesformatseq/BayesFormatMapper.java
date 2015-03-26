package com.ebusiness.assist.tobayesformatseq;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import net.paoding.analysis.analyzer.PaodingAnalyzer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.mahout.common.StringTuple;

import com.ebusiness.etl.transform.ITransformer;
import com.ebusiness.etl.transform.JsonTransformer;

public class BayesFormatMapper extends Mapper<Object, Text, Text, StringTuple> {

	private Text categoryId = new Text();
	
	private StringTuple outDocument = new StringTuple();
	
	
	private ITransformer transformer = new JsonTransformer();
	
	private HashMap<String, String> dataContainer = null ;
	
	private static String REPORT_GROUP = "REPORT_GROUP";
	
	private  PaodingAnalyzer analyzer = new PaodingAnalyzer();
	
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		dataContainer = transformer.getFormatData(value.toString());

		if (dataContainer == null)
			return;

		if (!StringUtils.isEmpty(dataContainer.get("category"))) {
			categoryId.set((dataContainer.get("category")));
		} else {
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		
		}
		
		
		if(!StringUtils.isEmpty(dataContainer.get("name"))){
			outDocument = tokenizer(dataContainer.get("name"));
			
		}else{
			context.getCounter(REPORT_GROUP, "null").increment(1);
			return;
		}
		
		/**
		 * 把类别标识符和相应的商品名称描述输出
		 */
		context.write(categoryId, outDocument);

	}
	
	private StringTuple tokenizer(String input){
		
	   StringTuple document = new StringTuple();
	   
       try {
           TokenStream ts = analyzer.tokenStream("", new StringReader(input));
           Token token;
           while ((token = ts.next()) != null) {
        	   //参考分词的源码，用new String，因为看不到token的源码，不知道会不会是和substring一样的结构，怕内存溢出
              document.add(new String(token.termText()));
           }
           return document;
       } catch (Exception e) {
           e.printStackTrace();
           return document;
       }
	
	}

}
