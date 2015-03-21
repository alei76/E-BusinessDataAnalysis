package com.ebusiness.report;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 数据分析报告的输出元组
 * @author Administrator
 *
 */
public class ReprotOutputTuple implements Writable {

	/**
	 * 商品类别ID
	 */
	private long categoryId;
	
	/**
	 * 此类别的最高售价
	 */
	private double maxPrice;
	
	
	/**
	 * 此类别的最低售价
	 */
	private double minPrice;
	
	/**
	 * 此类别的平均售价
	 */
	private double averagePrice;
	
	
	private String query;
	
	
	public void readFields(DataInput in) throws IOException {
		categoryId = in.readLong();
		maxPrice = in.readDouble();
		minPrice = in.readDouble();
		averagePrice = in.readDouble();
		query = in.readUTF();

	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(categoryId);
		out.writeDouble(maxPrice);
		out.writeDouble(minPrice);
		out.writeDouble(averagePrice);
		out.writeUTF(query);

	}

	public long getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(long categoryId) {
		this.categoryId = categoryId;
	}

	public double getMaxPrice() {
		return maxPrice;
	}

	public void setMaxPrice(double maxPrice) {
		this.maxPrice = maxPrice;
	}

	public double getMinPrice() {
		return minPrice;
	}

	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}

	

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public double getAveragePrice() {
		return averagePrice;
	}

	public void setAveragePrice(double averagePrice) {
		this.averagePrice = averagePrice;
	}

}
