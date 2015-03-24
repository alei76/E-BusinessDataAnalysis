package com.ebusiness.report.outputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.ebusiness.report.ReportOutputTuple;
import com.ebusiness.report.db.DBUtil;

public class DBRecordWriter extends RecordWriter<IntWritable, ReportOutputTuple> {

	private Connection conn = DBUtil.getConn();
	private Statement statement;
	
	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		DBUtil.release();
		
	}

	@Override
	public void write(IntWritable key, ReportOutputTuple value)
			throws IOException, InterruptedException {
		StringBuffer sql = new StringBuffer();
		sql.append(" insert into ebreport(categoryId,maxprice,minprice,avgprice,count)");
		sql.append(" values (").append(key).append(",").append(value.getMaxPrice()).append(",").append(value.getMinPrice());
		sql.append(",").append(value.getAveragePrice()).append(",").append(value.getCount()).append(")");
		
		try {
			statement = conn.createStatement();
			statement.executeUpdate(sql.toString());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
		
	}

}
