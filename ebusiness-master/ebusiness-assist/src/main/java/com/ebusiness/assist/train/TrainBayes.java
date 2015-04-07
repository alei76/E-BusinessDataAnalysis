package com.ebusiness.assist.train;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;

public class TrainBayes {

	public static void main(String[] args) throws Exception {
		
		 ToolRunner.run(new Configuration(), new TrainNaiveBayesJob(), args);
		 
	}

}
