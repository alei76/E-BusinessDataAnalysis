package com.ebusiness.assist.seq2vectors;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.hadoop.stats.BasicStats;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

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
		
		
		
		  Pair<Long[], List<Path>> docFrequenciesFeatures = null;
	      // Should document frequency features be processed
	   
		  boolean processIdf = true;
		  
	      if (shouldPrune || processIdf ) {
	   
	        docFrequenciesFeatures =
	                TFIDFConverter.calculateDF(new Path(outputDir, tfDirName), outputDir, conf, chunkSize);
	      }

	      long maxDF = maxDFPercent; //if we are pruning by std dev, then this will get changed
	      int minDf = 1 ;
		  if (shouldPrune) {
	        long vectorCount = docFrequenciesFeatures.getFirst()[1];
	        if (maxDFSigma >= 0.0) {
	          Path dfDir = new Path(outputDir, TFIDFConverter.WORDCOUNT_OUTPUT_FOLDER);
	          Path stdCalcDir = new Path(outputDir, HighDFWordsPruner.STD_CALC_DIR);

	          // Calculate the standard deviation
	          double stdDev = BasicStats.stdDevForGivenMean(dfDir, stdCalcDir, 0.0, conf);
	          maxDF = (int) (100.0 * maxDFSigma * stdDev / vectorCount);
	        }

	        long maxDFThreshold = (long) (vectorCount * (maxDF / 100.0f));

	        // Prune the term frequency vectors
	        Path tfDir = new Path(outputDir, tfDirName);
	        Path prunedTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
	        Path prunedPartialTFDir =
	                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-partial");
	       
	        if (processIdf) {
	          HighDFWordsPruner.pruneVectors(tfDir,
	                  prunedTFDir,
	                  prunedPartialTFDir,
	                  maxDFThreshold,
	                  minDf ,
	                  conf,
	                  docFrequenciesFeatures,
	                  -1.0f,
	                  false,
	                  reduceTasks);
	        } else {
	          HighDFWordsPruner.pruneVectors(tfDir,
	                  prunedTFDir,
	                  prunedPartialTFDir,
	                  maxDFThreshold,
	                  minDf,
	                  conf,
	                  docFrequenciesFeatures,
	                  norm,
	                  logNormalize,
	                  reduceTasks);
	        }
	        HadoopUtil.delete(new Configuration(conf), tfDir);
	      }
	      if (processIdf) {
	        TFIDFConverter.processTfIdf(
	                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
	                outputDir, conf, docFrequenciesFeatures, minDf, maxDF, norm, logNormalize,
	                sequentialAccessOutput, namedVectors, reduceTasks);
	      }
	}

}
