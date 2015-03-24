package com.mahout.test;

import org.apache.mahout.text.SequenceFilesFromDirectory;


public class TestSeqdirectory {

	public static void main(String[] args) throws Exception {
        String[] arg={"-fs","hdfs://hadoop:9000/","-jt","hdfs://hadoop:9001/",  
                "-i", "/tmp/mahout-work-root/20news-all",  
                "-o" ,"/tmp/mahout-work-root/20news-seq2"};  
        
        SequenceFilesFromDirectory.main(arg);  
	}

}
