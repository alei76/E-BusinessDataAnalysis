package com.ebusiness.etl.extract;



/**
 * 对InputExtract增强，实现具体的格式化操作,去掉输入字符串中的双引号
 * @author panzhichun
 *
 */
public class QuoteInputExtract extends InputExtract {


	InputExtract component;

	public QuoteInputExtract(InputExtract c) {
		component = c;
	}

	@Override
	public String extract(String input) {
		String value = component.extract(input);
		
		
		value =value.replaceAll("\"", "");// 去掉括号
		
		return value;
	}
	
}
