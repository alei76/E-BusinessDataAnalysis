package com.ebusiness.report;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 数据分析报告的输出元组
 * @author panzhichun
 *
 */
public class ReportOutputTuple implements Writable {

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
	
	
	
	
	private long count;
	
	
	public void readFields(DataInput in) throws IOException {
		categoryId = in.readLong();
		maxPrice = in.readDouble();
		minPrice = in.readDouble();
		averagePrice = in.readDouble();
		//query = in.readLine();
		count = in.readLong();

	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(categoryId);
		out.writeDouble(maxPrice);
		out.writeDouble(minPrice);
		out.writeDouble(averagePrice);
		
		out.writeLong(count);

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


	public double getAveragePrice() {
		return averagePrice;
	}

	public void setAveragePrice(double averagePrice) {
		this.averagePrice = averagePrice;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "[categoryId=" + categoryId + ", maxPrice="
				+ maxPrice + ", minPrice=" + minPrice + ", averagePrice="
				+ averagePrice + ", count=" + count + "]";
	}

}
