package com.ebusiness.assist.seq2vectors;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;

public class DicVectorizer {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		int maxDFPercent = 99;
		double maxDFSigma = -1.0;
		int minSupport = 2;
		int maxNGramSize = 1;
		float minLLRValue = LLRReducer.DEFAULT_MIN_LLR;
		float norm = -1.0f;
		boolean logNormalize = true;
		boolean sequentialAccessOutput = false;
		int reduceTasks = 1;
		int chunkSize = 100;
		boolean namedVectors = true;
		boolean shouldPrune = maxDFSigma >= 0.0 || maxDFPercent > 0.00;
		String tfDirName = shouldPrune ? DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER
				+ "-toprune"
				: DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER;

		
		Configuration conf = new Configuration();
		Path tokenizedPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		
		
		DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
				outputDir, tfDirName, conf, minSupport, maxNGramSize,
				minLLRValue, norm, logNormalize, reduceTasks, chunkSize,
				sequentialAccessOutput, namedVectors);
	}

}
