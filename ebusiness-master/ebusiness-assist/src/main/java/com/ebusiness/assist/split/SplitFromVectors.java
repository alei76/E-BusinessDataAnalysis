package com.ebusiness.assist.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.utils.SplitInput;


public class SplitFromVectors {

	public static void main(String[] args) throws Exception {
		
	
		ToolRunner.run(new Configuration(), new SplitInput(), args);
		
	}

}
